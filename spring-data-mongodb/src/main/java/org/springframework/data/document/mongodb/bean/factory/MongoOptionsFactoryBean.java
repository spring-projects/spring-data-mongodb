/*
 * Copyright 2010 the original author or authors.
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
package org.springframework.data.document.mongodb.bean.factory;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;


import com.mongodb.MongoOptions;

/**
 * A factory bean for consruction a MongoOptions instance
 * 
 * @author Graeme Rocher
 *
 */
public class MongoOptionsFactoryBean implements FactoryBean<MongoOptions>, InitializingBean{

	private static final MongoOptions MONGO_OPTIONS = new MongoOptions();
    /**
       number of connections allowed per host
       will block if run out
     */
    private int connectionsPerHost = MONGO_OPTIONS.connectionsPerHost;

    /**
       multiplier for connectionsPerHost for # of threads that can block
       if connectionsPerHost is 10, and threadsAllowedToBlockForConnectionMultiplier is 5, 
       then 50 threads can block
       more than that and an exception will be throw
     */
    private int threadsAllowedToBlockForConnectionMultiplier = MONGO_OPTIONS.threadsAllowedToBlockForConnectionMultiplier;
    
    /**
     * max wait time of a blocking thread for a connection
     */
    private int maxWaitTime = MONGO_OPTIONS.maxWaitTime;

    /**
       connect timeout in milliseconds. 0 is default and infinite
     */
    private int connectTimeout = MONGO_OPTIONS.connectTimeout;

    /**
       socket timeout.  0 is default and infinite
     */
    private int socketTimeout = MONGO_OPTIONS.socketTimeout;
    
    /**
       this controls whether or not on a connect, the system retries automatically 
    */
    private boolean autoConnectRetry = MONGO_OPTIONS.autoConnectRetry;
    
    
    /**
    	number of connections allowed per host
    	will block if run out
     */    
	public void setConnectionsPerHost(int connectionsPerHost) {
		this.connectionsPerHost = connectionsPerHost;
	}

    /**
    	multiplier for connectionsPerHost for # of threads that can block
    	if connectionsPerHost is 10, and threadsAllowedToBlockForConnectionMultiplier is 5, 
    	then 50 threads can block
    	more than that and an exception will be throw
     */	
	public void setThreadsAllowedToBlockForConnectionMultiplier(
			int threadsAllowedToBlockForConnectionMultiplier) {
		this.threadsAllowedToBlockForConnectionMultiplier = threadsAllowedToBlockForConnectionMultiplier;
	}

    /**
     * max wait time of a blocking thread for a connection
     */	
	public void setMaxWaitTime(int maxWaitTime) {
		this.maxWaitTime = maxWaitTime;
	}

    /**
    	connect timeout in milliseconds. 0 is default and infinite
     */
	public void setConnectTimeout(int connectTimeout) {
		this.connectTimeout = connectTimeout;
	}

    /**
    	socket timeout.  0 is default and infinite
     */	
	public void setSocketTimeout(int socketTimeout) {
		this.socketTimeout = socketTimeout;
	}

    /**
    	this controls whether or not on a connect, the system retries automatically 
     */	
	public void setAutoConnectRetry(boolean autoConnectRetry) {
		this.autoConnectRetry = autoConnectRetry;
	}

	public void afterPropertiesSet() throws Exception {
		MONGO_OPTIONS.connectionsPerHost = connectionsPerHost;
		MONGO_OPTIONS.threadsAllowedToBlockForConnectionMultiplier = threadsAllowedToBlockForConnectionMultiplier;
		MONGO_OPTIONS.maxWaitTime = maxWaitTime;
		MONGO_OPTIONS.connectTimeout = connectTimeout;
		MONGO_OPTIONS.socketTimeout = socketTimeout;
		MONGO_OPTIONS.autoConnectRetry = autoConnectRetry;
		
	}

	public MongoOptions getObject() throws Exception {
		return MONGO_OPTIONS;
	}

	public Class<?> getObjectType() {
		return MongoOptions.class;
	}

	public boolean isSingleton() {
		return true;
	}

}
