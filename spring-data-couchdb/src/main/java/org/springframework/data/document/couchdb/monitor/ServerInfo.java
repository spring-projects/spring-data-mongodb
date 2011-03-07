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
package org.springframework.data.document.couchdb.monitor;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.web.client.RestTemplate;

/**
 * Expose basic server information via JMX 
 * 
 * @author Mark Pollack
 *
 */
@ManagedResource(description="Server Information")
public class ServerInfo extends AbstractMonitor {
	
	
	public ServerInfo(String databaseUrl) {
		this.databaseUrl = databaseUrl;
		this.restTemplate = new RestTemplate();
	}
	

	@ManagedOperation(description="Server host name")
	public String getHostName() throws UnknownHostException {
		return InetAddress.getLocalHost().getHostName();
	}


	@ManagedOperation(description="CouchDB Server Version")
	public String getVersion() {
		return (String) getRoot().get("version");		
	}
	
	@ManagedOperation(description="Message of the day")
	public String getMotd() {
		return (String) getRoot().get("greeting");		
	}
	
	public Map getRoot() {
		Map map = restTemplate.getForObject(getDatabaseUrl(),Map.class);
		return map;
	}

}
