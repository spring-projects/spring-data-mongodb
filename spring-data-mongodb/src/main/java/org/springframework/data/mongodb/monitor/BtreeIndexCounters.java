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

import com.mongodb.DBObject;
import com.mongodb.Mongo;
import org.springframework.jmx.export.annotation.ManagedMetric;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.jmx.support.MetricType;

/**
 * JMX Metrics for B-tree index counters
 * 
 * @author Mark Pollack
 */
@ManagedResource(description = "Btree Metrics")
public class BtreeIndexCounters extends AbstractMonitor {

	public BtreeIndexCounters(Mongo mongo) {
		this.mongo = mongo;
	}

	@ManagedMetric(metricType = MetricType.GAUGE, displayName = "Accesses")
	public int getAccesses() {
		return getBtree("accesses");
	}

	@ManagedMetric(metricType = MetricType.GAUGE, displayName = "Hits")
	public int getHits() {
		return getBtree("hits");
	}

	@ManagedMetric(metricType = MetricType.GAUGE, displayName = "Misses")
	public int getMisses() {
		return getBtree("misses");
	}

	@ManagedMetric(metricType = MetricType.GAUGE, displayName = "Resets")
	public int getResets() {
		return getBtree("resets");
	}

	@ManagedMetric(metricType = MetricType.GAUGE, displayName = "Miss Ratio")
	public int getMissRatio() {
		return getBtree("missRatio");
	}

	private int getBtree(String key) {
		DBObject indexCounters = (DBObject) getServerStatus().get("indexCounters");
		if (indexCounters.get("note") != null) {
			String message = (String) indexCounters.get("note");
			if (message.contains("not supported")) {
				return -1;
			}
		}
		DBObject btree = (DBObject) indexCounters.get("btree");
		// Class c = btree.get(key).getClass();
		return (Integer) btree.get(key);
	}

}
