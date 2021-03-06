/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.util.trace;

import com.linkedin.pinot.common.request.InstanceRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrap a {@link Runnable} so that the thread executes this job
 * will be automatically registered/unregistered to/from a request.
 *
 */
public abstract class TraceRunnable implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(TraceRunnable.class);

  private final InstanceRequest request;
  private final Trace parent;

  private TraceRunnable(InstanceRequest request, Trace parent) {
    if (request == null) {
      LOGGER.warn("Passing null request to TraceRunnable, maybe forget to register the request in current thread.");
    }
    this.request = request;
    this.parent = parent;
  }

  /**
   * Only works when the calling thread has registered the requestId
   */
  public TraceRunnable() {
    this(TraceContext.getRequestForCurrentThread(), TraceContext.getLocalTraceForCurrentThread());
  }

  @Override
  public void run() {
    if (request != null) TraceContext.registerThreadToRequest(request, parent);
    try {
      runJob();
    } finally {
      if (request != null) TraceContext.unregisterThreadFromRequest();
    }
  }

  public abstract void runJob();

}
