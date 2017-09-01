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

package com.amazonaws.flink.refarch.utils;

import com.amazonaws.flink.refarch.events.TripEvent;
import com.amazonaws.flink.refarch.events.WatermarkEvent;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.LimitExceededException;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WatermarkTracker {
  private final String streamName;
  private final AmazonKinesis kinesisClient;

  private long currentWatermark;
  private long lastShardRefreshTime = 0;
  private List<Shard> shards = new ArrayList<>();

  private static final long SHARD_REFRESH_MILLIES = 10_000;
  private static final Logger LOG = LoggerFactory.getLogger(WatermarkTracker.class);

  private final PriorityBlockingQueue<TripEvent> inflightEvents = new PriorityBlockingQueue<>();


  public WatermarkTracker(String region, String streamName) {
    this.streamName = streamName;
    this.kinesisClient = AmazonKinesisClientBuilder.standard().withRegion(region).build();
  }


  public long sentWatermark(TripEvent nextEvent) {
    //determine the larges possible wartermark value
    refreshWatermark(nextEvent);

    //asynchronously ingest the watermark to every shard of the Kinesis stream
    new Thread(this::sentWatermark).start();

    return currentWatermark;
  }


  private void sentWatermark() {
    try {
      //refresh the list of available shards, if current state is too old
      if (System.currentTimeMillis() - lastShardRefreshTime >= SHARD_REFRESH_MILLIES) {
        refreshShards();

        lastShardRefreshTime = System.currentTimeMillis();
      }

      //send a watermark to every shard of the Kinesis stream
      shards.parallelStream()
          .map(shard -> new PutRecordRequest()
              .withStreamName(streamName)
              .withData(new WatermarkEvent(currentWatermark).payload)
              .withPartitionKey("23")
              .withExplicitHashKey(shard.getHashKeyRange().getStartingHashKey()))
          .map(kinesisClient::putRecord)
          .forEach(putRecordResult -> LOG.trace("send watermark {} to shard {}", new DateTime(currentWatermark), putRecordResult.getShardId()));

      LOG.debug("send watermark {}", new DateTime(currentWatermark));
    } catch (LimitExceededException | ProvisionedThroughputExceededException e) {

      //if any request is throttled, just wait for the next iteration to submit another watermark
      LOG.warn("skipping watermark due to limit exceeded exception");
    }
  }

  private void refreshWatermark(TripEvent nextEvent) {
    TripEvent oldestEvent = inflightEvents.poll();

    if (oldestEvent == null) {
      currentWatermark = nextEvent.timestamp - 1;
    } else {
      currentWatermark = oldestEvent.timestamp - 1;
    }
  }


  private void refreshShards() {
    DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
    describeStreamRequest.setStreamName(streamName);
    String exclusiveStartShardId = null;
    List<Shard> shards = new ArrayList<>();

    do {
      describeStreamRequest.setExclusiveStartShardId(exclusiveStartShardId);
      DescribeStreamResult describeStreamResult = kinesisClient.describeStream(describeStreamRequest);
      shards.addAll(describeStreamResult.getStreamDescription().getShards());

      if (describeStreamResult.getStreamDescription().getHasMoreShards() && shards.size() > 0) {
        exclusiveStartShardId = shards.get(shards.size() - 1).getShardId();
      } else {
        exclusiveStartShardId = null;
      }
    } while (exclusiveStartShardId != null);

    this.shards = shards;
  }


  public void trackTimestamp(ListenableFuture<UserRecordResult> f, TripEvent event) {
    //add event (and it's timestamp) to a priority queue and remove it when it has eventually been sent to the Kinesis stream
    Futures.addCallback(f, new RemoveTimestampCallback(event));
  }


  class RemoveTimestampCallback implements FutureCallback<UserRecordResult> {
    private final TripEvent event;

    RemoveTimestampCallback(TripEvent event) {
      this.event = event;

      inflightEvents.add(event);
    }

    private void removeEvent() {
      inflightEvents.remove(event);
    }

    @Override
    public void onFailure(Throwable t) {
      LOG.warn("failed to send event {}", event);

      removeEvent();
    }

    @Override
    public void onSuccess(UserRecordResult result) {
      removeEvent();
    }
  }
}
