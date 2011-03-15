/*
 * Copyright 2002-2011 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.document.mongodb.monitor;

import com.mongodb.DBObject;
import com.mongodb.Mongo;
import org.springframework.jmx.export.annotation.ManagedMetric;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.jmx.support.MetricType;

/**
 * JMX Metrics for Operation counters
 *
 * @author Mark Pollack
 */
@ManagedResource(description = "Operation Counters")
public class OperationCounters extends AbstractMonitor {


  public OperationCounters(Mongo mongo) {
    this.mongo = mongo;
  }

  @ManagedMetric(metricType = MetricType.COUNTER, displayName = "Insert operation count")
  public int getInsertCount() {
    return getOpCounter("insert");
  }

  @ManagedMetric(metricType = MetricType.COUNTER, displayName = "Query operation count")
  public int getQueryCount() {
    return getOpCounter("query");
  }

  @ManagedMetric(metricType = MetricType.COUNTER, displayName = "Update operation count")
  public int getUpdateCount() {
    return getOpCounter("update");
  }

  @ManagedMetric(metricType = MetricType.COUNTER, displayName = "Delete operation count")
  public int getDeleteCount() {
    return getOpCounter("delete");
  }

  @ManagedMetric(metricType = MetricType.COUNTER, displayName = "GetMore operation count")
  public int getGetMoreCount() {
    return getOpCounter("getmore");
  }

  @ManagedMetric(metricType = MetricType.COUNTER, displayName = "Command operation count")
  public int getCommandCount() {
    return getOpCounter("command");
  }

  private int getOpCounter(String key) {
    DBObject opCounters = (DBObject) getServerStatus().get("opcounters");
    return (Integer) opCounters.get(key);
  }
}
