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
 *
 */

package com.amazonaws.flink.refarch;

import com.amazonaws.flink.refarch.events.TripEvent;
import com.amazonaws.flink.refarch.utils.TaxiEventReader;
import com.amazonaws.flink.refarch.utils.WatermarkTracker;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.cli.*;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;


public class StreamPopulator
{
    private static final Logger LOG = LoggerFactory.getLogger(StreamPopulator.class);

    /** sent a watermark every WATERMARK_MILLIS ms or WATERMARK_EVENT_COUNT events, whatever comes first */
    private static final long WATERMARK_MILLIS = 5_000;
    private static final long WATERMARK_EVENT_COUNT = 100_000;

    /** sleep for at lease MIN_SLEEP_MILLIS if no events need to be sent to Kinesis */
    private static final long MIN_SLEEP_MILLIS = 5;

    /** print statistics every STAT_INTERVAL_MILLIS ms */
    private static final long STAT_INTERVAL_MILLIS = 60_000;

    private final String streamName;
    private final float speedupFactor;
    private final KinesisProducer kinesisProducer;
    private final TaxiEventReader taxiEventReader;
    private final WatermarkTracker watermarkTracker;


    public StreamPopulator(String region, String bucketName, String objectPrefix, String streamName, boolean aggregate, float speedupFactor) {
        KinesisProducerConfiguration producerConfiguration = new KinesisProducerConfiguration()
                .setRegion(region)
                .setCredentialsRefreshDelay(500)
                .setAggregationEnabled(aggregate);

        AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();

        this.streamName = streamName;
        this.speedupFactor = speedupFactor;
        this.kinesisProducer = new KinesisProducer(producerConfiguration);
        this.taxiEventReader = new TaxiEventReader(s3, bucketName, objectPrefix);
        this.watermarkTracker = new WatermarkTracker(region, streamName);

        LOG.info("Starting to populate stream {}", streamName);
    }


    public static void main( String[] args ) throws ParseException {
        Options options = new Options()
                .addOption("region", true, "the region containing the kinesis stream")
                .addOption("bucket", true, "the bucket containing the raw event data")
                .addOption("prefix", true, "the prefix of the objects containing the raw event data")
                .addOption("stream", true, "the name of the kinesis stream the events are sent to")
                .addOption("speedup", true, "the speedup factor for replaying events into the kinesis stream")
                .addOption("aggregate", "turn on aggregation of multiple events into a kinesis record")
                .addOption("seek", true, "start replaying events at given timestamp")
                .addOption("help", "print this help message");

        CommandLine line = new DefaultParser().parse(options, args);

        if (line.hasOption("help")) {
            new HelpFormatter().printHelp(MethodHandles.lookup().lookupClass().getName(), options);
        } else {
            StreamPopulator populator = new StreamPopulator(
                    line.getOptionValue("region", "eu-west-1"),
                    line.getOptionValue("bucket", "aws-bigdata-blog"),
                    line.getOptionValue("prefix", "artifacts/flink-refarch/data/"),
                    line.getOptionValue("stream", "taxi-trip-events"),
                    line.hasOption("aggregate"),
                    Float.valueOf(line.getOptionValue("speedup", "1440"))
            );

            if (line.hasOption("seek")) {
                populator.seek(new DateTime(line.getOptionValue("seek")).getMillis());
            }

            populator.populate();
        }
    }


    private void seek(long timestamp) {
        taxiEventReader.seek(timestamp);
    }


    private void populate() {
        long lastWatermark = 0, lastWatermarkSentTime = 0;
        long watermarkBatchEventCount = 0, statisticsBatchEventCount = 0;
        long statisticsLastOutputTimeslot = 0;

        TripEvent nextEvent = taxiEventReader.next();

        final long timeZeroSystem = System.currentTimeMillis();
        final long timeZeroLog = nextEvent.timestamp;

        while (true) {
            double timeDeltaSystem = (System.currentTimeMillis() - timeZeroSystem)*speedupFactor;
            long timeDeltaLog = nextEvent.timestamp - timeZeroLog;
            double replayTimeGap = timeDeltaSystem - timeDeltaLog;

            if (replayTimeGap < 0) {
                // wait until replay time has caught up with the time in the
                try {
                    long sleepTime = (long) Math.max(-replayTimeGap / speedupFactor, MIN_SLEEP_MILLIS);

                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    LOG.error(e.getMessage());
                }
            } else {
                //queue the next event for ingestion to the Kinesis stream
                ListenableFuture<UserRecordResult> f = kinesisProducer.addUserRecord(streamName, Integer.toString(nextEvent.hashCode()), nextEvent.payload);

                //monitor if the event has actually been sent and adapt the global watermark value accordingly
                watermarkTracker.trackTimestamp(f, nextEvent);

                watermarkBatchEventCount++;
                statisticsBatchEventCount++;

                LOG.trace("sent event {}", nextEvent);

                if (taxiEventReader.hasNext()) {
                    nextEvent = taxiEventReader.next();
                } else {
                    //terminate if there are no more events to replay
                    break;
                }
            }

            //emit a watermark to every shard of the Kinesis stream every WATERMARK_MILLIS or WATERMARK_EVENT_COUNT, whatever comes first
            if (System.currentTimeMillis() - lastWatermarkSentTime >= WATERMARK_MILLIS || watermarkBatchEventCount >= WATERMARK_EVENT_COUNT) {
                lastWatermark = watermarkTracker.sentWatermark(nextEvent);

                watermarkBatchEventCount = 0;
                lastWatermarkSentTime = System.currentTimeMillis();
            }


            if ((System.currentTimeMillis()-timeZeroSystem)/STAT_INTERVAL_MILLIS != statisticsLastOutputTimeslot) {
                double statisticsBatchEventRate = Math.round(1000.0 * statisticsBatchEventCount / STAT_INTERVAL_MILLIS);
                long replayLag = Math.round(replayTimeGap/speedupFactor/1000);

                LOG.info("all events with dropoff time before {} have been sent ({} events/sec, {} sec replay lag)", new DateTime(lastWatermark+1), statisticsBatchEventRate, replayLag);

                statisticsBatchEventCount = 0;
                statisticsLastOutputTimeslot = (System.currentTimeMillis()-timeZeroSystem)/STAT_INTERVAL_MILLIS;
            }
        }

        LOG.info("all events have been sent");

        kinesisProducer.flushSync();
        kinesisProducer.destroy();
    }
}