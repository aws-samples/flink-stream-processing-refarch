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

import com.amazonaws.flink.refarch.events.TripEvent;
import com.amazonaws.flink.refarch.utils.BackpressureSemaphore;
import com.amazonaws.flink.refarch.utils.TaxiEventReader;
import com.amazonaws.flink.refarch.utils.WatermarkTracker;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.common.util.concurrent.ListenableFuture;
import java.lang.invoke.MethodHandles;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StreamPopulator {
  private static final Logger LOG = LoggerFactory.getLogger(StreamPopulator.class);

  /** Sent a watermark every WATERMARK_MILLIS ms or WATERMARK_EVENT_COUNT events, whatever comes first. */
  private static final long WATERMARK_MILLIS = 5_000;
  private static final long WATERMARK_EVENT_COUNT = 100_000;

  /** Sleep for at lease MIN_SLEEP_MILLIS if no events need to be sent to Kinesis. */
  private static final long MIN_SLEEP_MILLIS = 5;

  /** Block process if number of locally buffered events exceeds MAX_OUTSTANDING_RECORD_COUNT. */
  private static final int MAX_OUTSTANDING_RECORD_COUNT = 50_000;


  private final String streamName;
  private final float speedupFactor;
  private final long statisticsFrequencyMillies;
  private final KinesisProducer kinesisProducer;
  private final TaxiEventReader taxiEventReader;
  private final WatermarkTracker watermarkTracker;
  private final BackpressureSemaphore<UserRecordResult> backpressureSemaphore;


  public StreamPopulator(String region, String bucketName, String objectPrefix, String streamName, boolean aggregate, float speedupFactor, long statisticsFrequencyMillies) {
    KinesisProducerConfiguration producerConfiguration = new KinesisProducerConfiguration()
        .setRegion(region)
        .setCredentialsRefreshDelay(500)
        .setRecordTtl(300_000)
        .setAggregationEnabled(aggregate);

    final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();

    this.streamName = streamName;
    this.speedupFactor = speedupFactor;
    this.statisticsFrequencyMillies = statisticsFrequencyMillies;
    this.kinesisProducer = new KinesisProducer(producerConfiguration);
    this.taxiEventReader = new TaxiEventReader(s3, bucketName, objectPrefix);
    this.watermarkTracker = new WatermarkTracker(region, streamName);
    this.backpressureSemaphore = new BackpressureSemaphore<>(MAX_OUTSTANDING_RECORD_COUNT);
  }


  public static void main(String[] args) throws ParseException {
    Options options = new Options()
        .addOption("region", true, "the region containing the kinesis stream")
        .addOption("bucket", true, "the bucket containing the raw event data")
        .addOption("prefix", true, "the prefix of the objects containing the raw event data")
        .addOption("stream", true, "the name of the kinesis stream the events are sent to")
        .addOption("speedup", true, "the speedup factor for replaying events into the kinesis stream")
        .addOption("aggregate", "turn on aggregation of multiple events into a kinesis record")
        .addOption("seek", true, "start replaying events at given timestamp")
        .addOption("statisticsFrequency", true, "print statistics every statisticFrequency ms")
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
          Float.valueOf(line.getOptionValue("speedup", "6480")),
          Long.valueOf(line.getOptionValue("statisticsFrequency", "60000"))
      );

      if (line.hasOption("seek")) {
        populator.seek(new DateTime(line.getOptionValue("seek")));
      }

      populator.populate();
    }
  }


  private void seek(DateTime timestamp) {
    LOG.info("skipping events with timestamps lower than {}", timestamp);

    taxiEventReader.seek(timestamp.getMillis());
  }


  private void populate() {
    long lastWatermark = 0;
    long lastWatermarkSentTime = 0;
    long watermarkBatchEventCount = 0;
    long statisticsBatchEventCount = 0;
    long statisticsLastOutputTimeslot = 0;

    TripEvent nextEvent = taxiEventReader.next();

    final long timeZeroSystem = System.currentTimeMillis();
    final long timeZeroLog = nextEvent.timestamp;

    LOG.info("starting to populate stream {}", streamName);

    while (true) {
      //determine system time, ie, how much time hast past since program invocation...
      double timeDeltaSystem = (System.currentTimeMillis() - timeZeroSystem) * speedupFactor;

      //determine event time, ie, how much time has passed according to the events that have been ingested to the Kinesis stream
      long timeDeltaLog = nextEvent.timestamp - timeZeroLog;

      double replayTimeGap = timeDeltaSystem - timeDeltaLog;

      if (replayTimeGap < 0) {
        //wait until event time has caught up with the system time
        try {
          long sleepTime = (long) Math.max(-replayTimeGap / speedupFactor, MIN_SLEEP_MILLIS);

          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          LOG.error(e.getMessage());
        }
      } else {
        //queue the next event for ingestion to the Kinesis stream through the KPL
        ListenableFuture<UserRecordResult> f = kinesisProducer.addUserRecord(
            streamName, Integer.toString(nextEvent.hashCode()), nextEvent.payload);

        //monitor if the event has actually been sent and adapt the largest possible watermark value accordingly
        watermarkTracker.trackTimestamp(f, nextEvent);

        //block if too many events are buffered locally
        backpressureSemaphore.acquire(f);

        watermarkBatchEventCount++;
        statisticsBatchEventCount++;

        LOG.trace("sent event {}", nextEvent);

        if (taxiEventReader.hasNext()) {
          //pre-fetch next event
          nextEvent = taxiEventReader.next();
        } else {
          //terminate if there are no more events to replay
          break;
        }
      }

      //emit a watermark to every shard of the Kinesis stream every WATERMARK_MILLIS ms or WATERMARK_EVENT_COUNT events, whatever comes first
      if (System.currentTimeMillis() - lastWatermarkSentTime >= WATERMARK_MILLIS || watermarkBatchEventCount >= WATERMARK_EVENT_COUNT) {
        lastWatermark = watermarkTracker.sentWatermark(nextEvent);

        watermarkBatchEventCount = 0;
        lastWatermarkSentTime = System.currentTimeMillis();
      }

      //output statistics every statisticsFrequencyMillies ms
      if ((System.currentTimeMillis() - timeZeroSystem) / statisticsFrequencyMillies != statisticsLastOutputTimeslot) {
        double statisticsBatchEventRate = Math.round(1000.0 * statisticsBatchEventCount / statisticsFrequencyMillies);
        long replayLag = Math.round(replayTimeGap / speedupFactor / 1000);

        LOG.info("all events with dropoff time before {} have been sent ({} events/sec, {} sec replay lag)",
            new DateTime(lastWatermark + 1), statisticsBatchEventRate, replayLag);

        statisticsBatchEventCount = 0;
        statisticsLastOutputTimeslot = (System.currentTimeMillis() - timeZeroSystem) / statisticsFrequencyMillies;
      }
    }

    LOG.info("all events have been sent");

    kinesisProducer.flushSync();
    kinesisProducer.destroy();
  }
}