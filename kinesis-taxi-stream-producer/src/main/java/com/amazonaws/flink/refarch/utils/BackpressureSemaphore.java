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

package com.amazonaws.flink.refarch.utils;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.Semaphore;

public class BackpressureSemaphore<T> implements FutureCallback<T> {

  private final Semaphore semaphore;

  public BackpressureSemaphore(int maxOutstandingRecordCount) {
    semaphore = new Semaphore(maxOutstandingRecordCount, true);
  }

  @Override
  public void onSuccess(T result) {
    semaphore.release();
  }

  @Override
  public void onFailure(Throwable t) {
    semaphore.release();
  }

  public void acquire(ListenableFuture<T> f) {
    try {
      semaphore.acquire();

      Futures.addCallback(f, this);
    } catch (InterruptedException e) {
      semaphore.release();
    }
  }
}
