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


  public TripEvent(String payload)  {
    super(payload);

    JsonNode json = Jackson.fromJsonString(this.payload, JsonNode.class);
    this.tripId = json.get(TRIP_ID).asLong();
    this.timestamp = new DateTime(json.get(DROPOFF_DATETIME).asText()).getMillis();
  }

  public TripEvent(String payload, DateTime timeOrigin) {
    this(shiftDropoffTime(payload, timeOrigin));
  }

  private static String shiftDropoffTime(String payload, DateTime timeOrigin) {
    ObjectNode json = (ObjectNode) Jackson.fromJsonString(payload, JsonNode.class);

    DateTime pickupTime = new DateTime(json.get(PICKUP_DATETIME).asText());
    DateTime dropoffTime = new DateTime(json.get(DROPOFF_DATETIME).asText());

    Duration timeDelta = new Duration(timeOrigin, pickupTime);

    json.put(PICKUP_DATETIME, pickupTime.minus(timeDelta).toString());
    json.put(DROPOFF_DATETIME, dropoffTime.minus(timeDelta).toString());

    return json.toString();
  }

  @Override
  public int compareTo(TripEvent that) {
    //ids are ordered by dropoffTime, so this effectively orders TripEvents by dropoffTime
    return Long.compare(this.tripId, that.tripId);
  }
}
