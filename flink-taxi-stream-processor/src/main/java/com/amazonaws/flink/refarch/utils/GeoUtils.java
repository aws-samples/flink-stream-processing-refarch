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

import ch.hsr.geohash.GeoHash;
import ch.hsr.geohash.WGS84Point;
import com.amazonaws.flink.refarch.events.kinesis.TripEvent;
import com.google.common.collect.ImmutableList;
import java.util.List;


public class GeoUtils {

  private static final List<GeoHash> NYC = ImmutableList.copyOf(GeoHash.fromGeohashString("dr72").getAdjacent());
  private static final List<GeoHash> JFK = ImmutableList.copyOf(GeoHash.fromGeohashString("dr5x0z").getAdjacent());
  private static final List<GeoHash> LGA = ImmutableList.<GeoHash>builder()
      .add(GeoHash.fromGeohashString("dr5ryy"))
      .add(GeoHash.fromGeohashString("dr5rzn"))
      .add(GeoHash.fromGeohashString("dr5rzjx").getAdjacent())
      .build();

  public static boolean nearNYC(TripEvent trip) {
    return GeoUtils.nearNYC(trip.pickup_lat, trip.pickup_lon)
        && GeoUtils.nearNYC(trip.dropoff_lat, trip.dropoff_lon);
  }

  public static boolean nearNYC(double latitude, double longitude) {
    return NYC.stream().anyMatch(hash -> hash.contains(new WGS84Point(latitude, longitude)));
  }

  public static boolean nearJFK(double latitude, double longitude) {
    return JFK.stream().anyMatch(hash -> hash.contains(new WGS84Point(latitude, longitude)));
  }

  public static boolean nearLGA(double latitude, double longitude) {
    return LGA.stream().anyMatch(hash -> hash.contains(new WGS84Point(latitude, longitude)));
  }

  public static boolean hasValidCoordinates(TripEvent trip) {
    return Math.abs(trip.pickup_lat) <= 90 && Math.abs(trip.pickup_lon) <= 180
        && Math.abs(trip.dropoff_lat) <= 90 && Math.abs(trip.dropoff_lon) <= 180;
  }
}
