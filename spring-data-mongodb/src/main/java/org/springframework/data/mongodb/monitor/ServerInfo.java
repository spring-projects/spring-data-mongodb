/*
 * Copyright 2012-2019 the original author or authors.
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

import java.net.UnknownHostException;

import org.springframework.jmx.export.annotation.ManagedMetric;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.jmx.support.MetricType;
import org.springframework.util.StringUtils;

import com.mongodb.client.MongoClient;

/**
 * Expose basic server information via JMX
 *
 * @author Mark Pollack
 * @author Thomas Darimont
 * @author Christoph Strobl
 */
@ManagedResource(description = "Server Information")
public class ServerInfo extends AbstractMonitor {

	/**
	 * @param mongoClient
	 * @since 2.2
	 */
	protected ServerInfo(MongoClient mongoClient) {
		super(mongoClient);
	}

	/**
	 * Returns the hostname of the used server reported by MongoDB.
	 *
	 * @return the reported hostname can also be an IP address.
	 * @throws UnknownHostException
	 */
	@ManagedOperation(description = "Server host name")
	public String getHostName() throws UnknownHostException {

		/*
		 * UnknownHostException is not necessary anymore, but clients could have
		 * called this method in a try..catch(UnknownHostException) already
		 */
		return StringUtils.collectionToDelimitedString(hosts(), ",");
	}

	@ManagedMetric(displayName = "Uptime Estimate")
	public double getUptimeEstimate() {
		return (Double) getServerStatus().get("uptimeEstimate");
	}

	@ManagedOperation(description = "MongoDB Server Version")
	public String getVersion() {
		return (String) getServerStatus().get("version");
	}

	@ManagedOperation(description = "Local Time")
	public String getLocalTime() {
		return (String) getServerStatus().get("localTime");
	}

	@ManagedMetric(metricType = MetricType.COUNTER, displayName = "Server uptime in seconds", unit = "seconds")
	public double getUptime() {
		return (Double) getServerStatus().get("uptime");
	}
}
