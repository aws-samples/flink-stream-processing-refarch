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

package com.amazonaws.flink.refarch.events.kinesis;

import org.joda.time.DateTime;


public class TripEvent extends Event {
  public final double pickup_lat;
  public final double pickup_lon;
  public final double dropoff_lat;
  public final double dropoff_lon;
  public final double total_amount;
  public final DateTime pickup_datetime;
  public final DateTime dropoff_datetime;

  public TripEvent() {
    pickup_lat = 0;
    pickup_lon = 0;
    dropoff_lat = 0;
    dropoff_lon = 0;
    total_amount = 0;
    pickup_datetime = DateTime.now();
    dropoff_datetime = DateTime.now();
  }

  @Override
  public long getTimestamp() {
    return dropoff_datetime.getMillis();
  }
}
