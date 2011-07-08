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
package org.springframework.data.mongodb.monitor;

import java.util.Date;

import com.mongodb.DBObject;
import com.mongodb.Mongo;
import org.springframework.jmx.export.annotation.ManagedMetric;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.jmx.support.MetricType;

/**
 * JMX Metrics for Background Flushing
 * 
 * @author Mark Pollack
 */
@ManagedResource(description = "Background Flushing Metrics")
public class BackgroundFlushingMetrics extends AbstractMonitor {

	public BackgroundFlushingMetrics(Mongo mongo) {
		this.mongo = mongo;
	}

	@ManagedMetric(metricType = MetricType.COUNTER, displayName = "Flushes")
	public int getFlushes() {
		return getFlushingData("flushes", java.lang.Integer.class);
	}

	@ManagedMetric(metricType = MetricType.COUNTER, displayName = "Total ms", unit = "ms")
	public int getTotalMs() {
		return getFlushingData("total_ms", java.lang.Integer.class);
	}

	@ManagedMetric(metricType = MetricType.GAUGE, displayName = "Average ms", unit = "ms")
	public double getAverageMs() {
		return getFlushingData("average_ms", java.lang.Double.class);
	}

	@ManagedMetric(metricType = MetricType.GAUGE, displayName = "Last Ms", unit = "ms")
	public int getLastMs() {
		return getFlushingData("last_ms", java.lang.Integer.class);
	}

	@ManagedMetric(metricType = MetricType.GAUGE, displayName = "Last finished")
	public Date getLastFinished() {
		return getLast();
	}

	@SuppressWarnings("unchecked")
	private <T> T getFlushingData(String key, Class<T> targetClass) {
		DBObject mem = (DBObject) getServerStatus().get("backgroundFlushing");
		return (T) mem.get(key);
	}

	private Date getLast() {
		DBObject bgFlush = (DBObject) getServerStatus().get("backgroundFlushing");
		Date lastFinished = (Date) bgFlush.get("last_finished");
		return lastFinished;
	}

}
