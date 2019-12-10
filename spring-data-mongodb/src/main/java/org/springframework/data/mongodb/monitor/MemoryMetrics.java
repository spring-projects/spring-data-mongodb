/*
 * Copyright 2002-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.monitor;

import org.bson.Document;
import org.springframework.jmx.export.annotation.ManagedMetric;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.jmx.support.MetricType;

import com.mongodb.client.MongoClient;

/**
 * JMX Metrics for Memory
 *
 * @author Mark Pollack
 */
@ManagedResource(description = "Memory Metrics")
public class MemoryMetrics extends AbstractMonitor {

	/**
	 * @param mongoClient
	 * @since 2.2
	 */
	public MemoryMetrics(MongoClient mongoClient) {
		super(mongoClient);
	}

	@ManagedMetric(metricType = MetricType.COUNTER, displayName = "Memory address size")
	public int getBits() {
		return getMemData("bits", java.lang.Integer.class);
	}

	@ManagedMetric(metricType = MetricType.GAUGE, displayName = "Resident in Physical Memory", unit = "MB")
	public int getResidentSpace() {
		return getMemData("resident", java.lang.Integer.class);
	}

	@ManagedMetric(metricType = MetricType.GAUGE, displayName = "Virtual Address Space", unit = "MB")
	public int getVirtualAddressSpace() {
		return getMemData("virtual", java.lang.Integer.class);
	}

	@ManagedMetric(metricType = MetricType.GAUGE, displayName = "Is memory info supported on this platform")
	public boolean getMemoryInfoSupported() {
		return getMemData("supported", java.lang.Boolean.class);
	}

	@ManagedMetric(metricType = MetricType.GAUGE, displayName = "Memory Mapped Space", unit = "MB")
	public int getMemoryMappedSpace() {
		return getMemData("mapped", java.lang.Integer.class);
	}

	@SuppressWarnings("unchecked")
	private <T> T getMemData(String key, Class<T> targetClass) {
		Document mem = (Document) getServerStatus().get("mem");
		// Class c = mem.get(key).getClass();
		return (T) mem.get(key);
	}

}
