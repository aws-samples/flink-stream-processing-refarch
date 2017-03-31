/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may
 * not use this file except in compliance with the License. A copy of the
 * License is located at
 *
 *    http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazonaws.flink.refarch;

import ch.hsr.geohash.GeoHash;
import com.amazonaws.flink.refarch.events.EventSchema;
import com.amazonaws.flink.refarch.events.PunctuatedAssigner;
import com.amazonaws.flink.refarch.events.es.PickupCount;
import com.amazonaws.flink.refarch.events.es.TripDuration;
import com.amazonaws.flink.refarch.events.kinesis.Event;
import com.amazonaws.flink.refarch.events.kinesis.TripEvent;
import com.amazonaws.flink.refarch.utils.ElasticsearchJestSink;
import com.amazonaws.flink.refarch.utils.GeoUtils;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.util.Collector;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.stream.StreamSupport;


public class ProcessTaxiStream {
    private static final String DEFAULT_REGION = "eu-west-1";
    private static final String DEFAULT_STREAM_NAME = "taxi-trip-events";
    private static final String ES_DEFAULT_INDEX = "taxi-dashboard";

    private static final int MIN_PICKUP_COUNT = 2;
    private static final int GEOHASH_PRECISION = 6;

    private static final Logger LOG = LoggerFactory.getLogger(ProcessTaxiStream.class);

    public static void main(String[] args) throws Exception {
        ParameterTool pt = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        Properties kinesisConsumerConfig = new Properties();
        kinesisConsumerConfig.setProperty(AWSConfigConstants.AWS_REGION, pt.get("region", DEFAULT_REGION));
        kinesisConsumerConfig.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "AUTO");
        kinesisConsumerConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_MAX, "10000");
        kinesisConsumerConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_BACKOFF_BASE, "500");
        kinesisConsumerConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS, "200");

        DataStream<Event> kinesisStream = env.addSource(new FlinkKinesisConsumer<>(
                pt.get("stream", DEFAULT_STREAM_NAME),
                new EventSchema(),
                kinesisConsumerConfig)
        );


        DataStream<TripEvent> trips = kinesisStream
                .assignTimestampsAndWatermarks(new PunctuatedAssigner())
                .filter(event -> TripEvent.class.isAssignableFrom(event.getClass()))
                .map(event -> (TripEvent) event)
                .filter(GeoUtils::hasValidCoordinates)
                .filter(GeoUtils::nearNYC);


        DataStream<PickupCount> pickupCount = trips
                .map(trip -> new Tuple1<>(GeoHash.geoHashStringWithCharacterPrecision(trip.pickup_lat, trip.pickup_lon, GEOHASH_PRECISION)))
                .keyBy(0)
                .timeWindow(Time.minutes(10))
                .apply((Tuple tuple, TimeWindow window, Iterable<Tuple1<String>> input, Collector<PickupCount> out) -> {
                    long count = Iterables.size(input);
                    String position = Iterables.get(input, 0).f0;

                    out.collect(new PickupCount(position, count, window.maxTimestamp()));
                })
                .filter(geo -> geo.pickup_count >= MIN_PICKUP_COUNT);


        DataStream<TripDuration> tripDuration = trips.
                flatMap((TripEvent trip, Collector<Tuple3<String, String, Long>> out) -> {
                    String pickup_location = GeoHash.geoHashStringWithCharacterPrecision(trip.pickup_lat, trip.pickup_lon, 5);
                    long trip_duration = new Duration(trip.pickup_datetime, trip.dropoff_datetime).getStandardMinutes();

                    if (GeoUtils.nearJFK(trip.dropoff_lat, trip.dropoff_lon)) {
                        out.collect(new Tuple3<>(pickup_location, "JFK", trip_duration));
                    } else if (GeoUtils.nearLGA(trip.dropoff_lat, trip.dropoff_lon)) {
                        out.collect(new Tuple3<>(pickup_location, "LGA", trip_duration));
                    }
                })
                .keyBy(0,1)
                .timeWindow(Time.minutes(10))
                .apply((Tuple tuple, TimeWindow window, Iterable<Tuple3<String,String,Long>> input, Collector<TripDuration> out) -> {
                    if (Iterables.size(input) > 1) {
                        String location = Iterables.get(input, 0).f0;
                        String airportCode = Iterables.get(input, 0).f1;

                        long sumDuration = StreamSupport
                                .stream(input.spliterator(), false)
                                .mapToLong(trip -> trip.f2)
                                .sum();

                        double avgDuration = (double) sumDuration / Iterables.size(input);

                        out.collect(new TripDuration(location, airportCode, sumDuration, avgDuration, window.maxTimestamp()));
                    }
                });


        if (pt.has("checkpoint")) {
            env.enableCheckpointing(5000);
        }

        if (pt.has("es-endpoint")) {
            final String indexName = pt.get("es-index", ES_DEFAULT_INDEX);

            final ImmutableMap<String, String> config = ImmutableMap.<String, String>builder()
                    .put("es-endpoint", pt.getRequired("es-endpoint"))
                    .put("region", pt.get("region", DEFAULT_REGION))
                    .build();

            pickupCount.addSink(new ElasticsearchJestSink<>(config, indexName, "pickup_count"));
            tripDuration.addSink(new ElasticsearchJestSink<>(config, indexName, "trip_duration"));
        }

        LOG.info("Starting to consume events from stream {}", pt.get("stream", DEFAULT_STREAM_NAME));

        env.execute();
    }
}
