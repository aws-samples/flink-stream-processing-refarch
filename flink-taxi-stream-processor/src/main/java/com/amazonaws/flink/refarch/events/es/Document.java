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

import com.google.gson.Gson;


public abstract class Document {
  private static final Gson gson = new Gson();

  public final long timestamp;

  public Document(long timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public String toString() {
    return gson.toJson(this);
  }
}
