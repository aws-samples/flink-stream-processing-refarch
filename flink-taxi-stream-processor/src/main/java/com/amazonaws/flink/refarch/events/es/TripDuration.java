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

package com.amazonaws.flink.refarch.events.es;


public class TripDuration extends Document {
  public final String location;
  public final String airport_code;
  public final long sum_trip_duration;
  public final double avg_trip_duration;

  public TripDuration(String location, String airport_code, long sum_trip_duration, double avg_trip_duration, long timestamp) {
    super(timestamp);

    this.location = location;
    this.airport_code = airport_code;
    this.avg_trip_duration = avg_trip_duration;
    this.sum_trip_duration = sum_trip_duration;
  }
}