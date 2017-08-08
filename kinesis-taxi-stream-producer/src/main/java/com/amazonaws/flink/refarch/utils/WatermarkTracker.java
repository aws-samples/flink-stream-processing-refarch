package com.amazonaws.flink.refarch.utils;

import com.amazonaws.flink.refarch.events.TripEvent;
import com.amazonaws.flink.refarch.events.WatermarkEvent;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.*;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by shausma on 8/8/17.
 */
public class WatermarkTracker {
    private final String streamName;
    private final AmazonKinesis kinesisClient;

    private long currentWatermark;
    private long lastShardRefreshTime = 0;
    private List<Shard> shards = new ArrayList<>();

    private final long SHARD_REFRESH_MILLIES = 10_000;
    private static final Logger LOG = LoggerFactory.getLogger(WatermarkTracker.class);

    private final SortedSet<TripEvent> inflightEvents = Collections.synchronizedSortedSet(new TreeSet<>());


    public WatermarkTracker(String region, String streamName) {
        this.streamName = streamName;
        this.kinesisClient = AmazonKinesisClientBuilder.standard().withRegion(region).build();
    }


    public long sentWatermark(TripEvent nextEvent) {
        refreshWatermark(nextEvent);

        new Thread(this::sentWatermark).start();

        return currentWatermark;
    }


    private void sentWatermark() {
        try {
            if (System.currentTimeMillis() - lastShardRefreshTime >= SHARD_REFRESH_MILLIES) {
                refreshShards();

                lastShardRefreshTime = System.currentTimeMillis();
            }

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
            LOG.warn("skipping watermark due to limit exceeded exception");
        }
    }


    private void refreshWatermark(TripEvent nextEvent) {
        try {
            currentWatermark = inflightEvents.first().timestamp - 1;
        } catch (NoSuchElementException e) {
            currentWatermark = nextEvent.timestamp - 1;
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
