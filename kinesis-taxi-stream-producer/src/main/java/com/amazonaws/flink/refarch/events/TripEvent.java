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

package com.amazonaws.flink.refarch.events;

import com.amazonaws.util.json.Jackson;
import com.fasterxml.jackson.databind.JsonNode;
import org.joda.time.DateTime;


public class TripEvent extends Event implements Comparable<TripEvent> {
    public final long tripId;
    public final long timestamp;

    public TripEvent(String payload)  {
        super(payload);

        JsonNode json = Jackson.fromJsonString(payload, JsonNode.class);
        this.tripId = json.get("trip_id").asLong();
        this.timestamp = new DateTime(json.get("dropoff_datetime").asText()).getMillis();
    }

    @Override
    public int compareTo(TripEvent that) {
        return Long.compare(this.tripId, that.tripId);             //ids are ordered by dropoff time
    }
}
