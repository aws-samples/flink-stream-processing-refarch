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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.internal.Streams;
import com.google.gson.stream.JsonReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import org.joda.time.DateTime;


public abstract class Event {
  private static final String TYPE_FIELD = "type";

  private static final Gson gson = new GsonBuilder()
      .registerTypeAdapter(DateTime.class, (JsonDeserializer<DateTime>) (json, typeOfT, context) -> new DateTime(json.getAsString()))
      .create();

  public static Event parseEvent(byte[] event) {
    JsonReader jsonReader =  new JsonReader(new InputStreamReader(new ByteArrayInputStream(event)));
    JsonElement jsonElement = Streams.parse(jsonReader);
    JsonElement labelJsonElement = jsonElement.getAsJsonObject().remove(TYPE_FIELD);

    if (labelJsonElement == null) {
      throw new IllegalArgumentException("Event does not define a type field: " + new String(event));
    }

    switch (labelJsonElement.getAsString()) {
      case "watermark":
        return gson.fromJson(jsonElement, WatermarkEvent.class);
      case "trip":
        return gson.fromJson(jsonElement, TripEvent.class);
      default:
        throw new IllegalArgumentException("Found unsupported event type: " + labelJsonElement.getAsString());
    }
  }

  public abstract long getTimestamp();
}