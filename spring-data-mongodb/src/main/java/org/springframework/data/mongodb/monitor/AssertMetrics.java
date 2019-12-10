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
 * JMX Metrics for assertions
 *
 * @author Mark Pollack
 */
@ManagedResource(description = "Assertion Metrics")
public class AssertMetrics extends AbstractMonitor {

	/**
	 * @param mongoClient
	 * @since 2.2
	 */
	public AssertMetrics(MongoClient mongoClient) {
		super(mongoClient);
	}

	@ManagedMetric(metricType = MetricType.COUNTER, displayName = "Regular")
	public int getRegular() {
		return getBtree("regular");
	}

	@ManagedMetric(metricType = MetricType.COUNTER, displayName = "Warning")
	public int getWarning() {
		return getBtree("warning");
	}

	@ManagedMetric(metricType = MetricType.COUNTER, displayName = "Msg")
	public int getMsg() {
		return getBtree("msg");
	}

	@ManagedMetric(metricType = MetricType.COUNTER, displayName = "User")
	public int getUser() {
		return getBtree("user");
	}

	@ManagedMetric(metricType = MetricType.GAUGE, displayName = "Rollovers")
	public int getRollovers() {
		return getBtree("rollovers");
	}

	private int getBtree(String key) {
		Document asserts = (Document) getServerStatus().get("asserts");
		// Class c = btree.get(key).getClass();
		return (Integer) asserts.get(key);
	}

}
