/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazonaws.flink.refarch.events.kinesis.Event;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;


public class EventSchema implements DeserializationSchema<Event> {

  @Override
  public Event deserialize(byte[] bytes) {
    return Event.parseEvent(bytes);
  }

  @Override
  public boolean isEndOfStream(Event event) {
    return false;
  }

  @Override
  public TypeInformation<Event> getProducedType() {
    return TypeExtractor.getForClass(Event.class);
  }

}
