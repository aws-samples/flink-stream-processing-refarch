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

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Objects;


public class Event {
  public static final String TYPE_FIELD = "type";

  public final String payload;

  public Event(String payload) {
    if (!payload.endsWith("\n")) {
      //append a newline to output to make it easier digestible by firehose and athena
      this.payload = payload + "\n";
    } else {
      this.payload = payload;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(payload);
  }

  @Override
  public String toString() {
    return payload;
  }

  public ByteBuffer toByteBuffer() {
    return ByteBuffer.wrap(payload.getBytes(Charset.forName("UTF-8")));
  }
}
