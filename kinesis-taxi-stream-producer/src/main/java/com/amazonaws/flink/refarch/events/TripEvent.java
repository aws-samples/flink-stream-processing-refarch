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

package com.amazonaws.flink.refarch.events;

import com.amazonaws.flink.refarch.AdaptTimeOption;
import com.amazonaws.util.json.Jackson;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.joda.time.DateTime;
import org.joda.time.Duration;


public class TripEvent extends Event implements Comparable<TripEvent> {
  public final long tripId;
  public final long timestamp;

  private final static String TRIP_ID = "trip_id";
  private final static String DROPOFF_DATETIME = "dropoff_datetime";
  private final static String PICKUP_DATETIME = "pickup_datetime";

  private final static Duration DELTA_TO_FIRST_DROPOFF_TIME = new Duration(new DateTime("2016-01-01T00:00:00.000Z"), DateTime.now());


  public TripEvent(String payload)  {
    super(payload);

    JsonNode json = Jackson.fromJsonString(this.payload, JsonNode.class);
    this.tripId = json.get(TRIP_ID).asLong();
    this.timestamp = new DateTime(json.get(DROPOFF_DATETIME).asText()).getMillis();
  }

  public static TripEvent adaptTime(TripEvent event, AdaptTimeOption adaptTimeOption) {
    switch (adaptTimeOption) {
      case ORIGINAL:
        return event;
      case INVOCATION:
        return fromStringShiftOrigin(event.payload, DELTA_TO_FIRST_DROPOFF_TIME);
      case INGESTION:
        return fromStringOverwriteTime(event.payload);
      default:
        throw new IllegalArgumentException();
    }
  }

  public static TripEvent fromStringShiftOrigin(String payload, Duration timeDelta) {
    ObjectNode json = (ObjectNode) Jackson.fromJsonString(payload, JsonNode.class);

    DateTime pickupTime = new DateTime(json.get(PICKUP_DATETIME).asText());
    DateTime dropoffTime = new DateTime(json.get(DROPOFF_DATETIME).asText());

    json.put(PICKUP_DATETIME, pickupTime.plus(timeDelta).toString());
    json.put(DROPOFF_DATETIME, dropoffTime.plus(timeDelta).toString());

    return new TripEvent(json.toString());
  }

  public static TripEvent fromStringOverwriteTime(String payload) {
    ObjectNode json = (ObjectNode) Jackson.fromJsonString(payload, JsonNode.class);

    DateTime pickupTime = new DateTime(json.get(PICKUP_DATETIME).asText());
    DateTime dropoffTime = new DateTime(json.get(DROPOFF_DATETIME).asText());

    Duration timeDelta = new Duration(dropoffTime, DateTime.now());

    json.put(PICKUP_DATETIME, pickupTime.plus(timeDelta).toString());
    json.put(DROPOFF_DATETIME, dropoffTime.plus(timeDelta).toString());

    return new TripEvent(json.toString());
  }

  @Override
  public int compareTo(TripEvent that) {
    //ids are ordered by dropoffTime, so this effectively orders TripEvents by dropoffTime
    return Long.compare(this.tripId, that.tripId);
  }
}
