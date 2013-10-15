/*
 * Copyright 2010-2013 the original author or authors.
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
package org.springframework.data.mongodb.core;

import javax.net.ssl.SSLSocketFactory;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

import com.mongodb.MongoOptions;

/**
 * A factory bean for construction of a {@link MongoOptions} instance.
 * 
 * @author Graeme Rocher
 * @author Mark Pollack
 * @author Mike Saavedra
 * @author Thomas Darimont
 */
public class MongoOptionsFactoryBean implements FactoryBean<MongoOptions>, InitializingBean {

	@SuppressWarnings("deprecation")//
	private final MongoOptions MONGO_OPTIONS = new MongoOptions();

	private int connectionsPerHost = MONGO_OPTIONS.connectionsPerHost;

	private int threadsAllowedToBlockForConnectionMultiplier = MONGO_OPTIONS.threadsAllowedToBlockForConnectionMultiplier;
	private int maxWaitTime = MONGO_OPTIONS.maxWaitTime;
	private int connectTimeout = MONGO_OPTIONS.connectTimeout;
	private int socketTimeout = MONGO_OPTIONS.socketTimeout;
	private boolean socketKeepAlive = MONGO_OPTIONS.socketKeepAlive;
	private boolean autoConnectRetry = MONGO_OPTIONS.autoConnectRetry;
	private long maxAutoConnectRetryTime = MONGO_OPTIONS.maxAutoConnectRetryTime;
	private int writeNumber;
	private int writeTimeout;
	private boolean writeFsync;
	@SuppressWarnings("deprecation") private boolean slaveOk = MONGO_OPTIONS.slaveOk;
	private boolean ssl;
	private SSLSocketFactory sslSocketFactory;

	/**
	 * Configures the maximum number of connections allowed per host until we will block.
	 * 
	 * @param connectionsPerHost
	 */
	public void setConnectionsPerHost(int connectionsPerHost) {
		this.connectionsPerHost = connectionsPerHost;
	}

	/**
	 * A multiplier for connectionsPerHost for # of threads that can block a connection. If connectionsPerHost is 10, and
	 * threadsAllowedToBlockForConnectionMultiplier is 5, then 50 threads can block. If more threads try to block an
	 * exception will be thrown.
	 * 
	 * @param threadsAllowedToBlockForConnectionMultiplier
	 */
	public void setThreadsAllowedToBlockForConnectionMultiplier(int threadsAllowedToBlockForConnectionMultiplier) {
		this.threadsAllowedToBlockForConnectionMultiplier = threadsAllowedToBlockForConnectionMultiplier;
	}

	/**
	 * Max wait time of a blocking thread for a connection.
	 * 
	 * @param maxWaitTime
	 */
	public void setMaxWaitTime(int maxWaitTime) {
		this.maxWaitTime = maxWaitTime;
	}

	/**
	 * Configures the connect timeout in milliseconds. Defaults to 0 (infinite time).
	 * 
	 * @param connectTimeout
	 */
	public void setConnectTimeout(int connectTimeout) {
		this.connectTimeout = connectTimeout;
	}

	/**
	 * Configures the socket timeout. Defaults to 0 (infinite time).
	 * 
	 * @param socketTimeout
	 */
	public void setSocketTimeout(int socketTimeout) {
		this.socketTimeout = socketTimeout;
	}

	/**
	 * Configures whether or not to have socket keep alive turned on (SO_KEEPALIVE). Defaults to {@literal false}.
	 * 
	 * @param socketKeepAlive
	 */
	public void setSocketKeepAlive(boolean socketKeepAlive) {
		this.socketKeepAlive = socketKeepAlive;
	}

	/**
	 * This specifies the number of servers to wait for on the write operation, and exception raising behavior. The 'w'
	 * option to the getlasterror command. Defaults to 0.
	 * <ul>
	 * <li>-1 = don't even report network errors</li>
	 * <li>0 = default, don't call getLastError by default</li>
	 * <li>1 = basic, call getLastError, but don't wait for slaves</li>
	 * <li>2 += wait for slaves</li>
	 * </ul>
	 * 
	 * @param writeNumber the number of servers to wait for on the write operation, and exception raising behavior.
	 */
	public void setWriteNumber(int writeNumber) {
		this.writeNumber = writeNumber;
	}

	/**
	 * Configures the timeout for write operations in milliseconds. This defaults to {@literal 0} (indefinite).
	 * 
	 * @param writeTimeout
	 */
	public void setWriteTimeout(int writeTimeout) {
		this.writeTimeout = writeTimeout;
	}

	/**
	 * Configures whether or not to fsync. The 'fsync' option to the getlasterror command. Defaults to {@literal false}.
	 * 
	 * @param writeFsync to fsync on <code>write (true)<code>, otherwise {@literal false}.
	 */
	public void setWriteFsync(boolean writeFsync) {
		this.writeFsync = writeFsync;
	}

	/**
	 * Configures whether or not the system retries automatically on a failed connect. This defaults to {@literal false}.
	 */
	public void setAutoConnectRetry(boolean autoConnectRetry) {
		this.autoConnectRetry = autoConnectRetry;
	}

	/**
	 * Configures the maximum amount of time in millisecons to spend retrying to open connection to the same server. This
	 * defaults to {@literal 0}, which means to use the default {@literal 15s} if {@link #autoConnectRetry} is on.
	 * 
	 * @param maxAutoConnectRetryTime the maxAutoConnectRetryTime to set
	 */
	public void setMaxAutoConnectRetryTime(long maxAutoConnectRetryTime) {
		this.maxAutoConnectRetryTime = maxAutoConnectRetryTime;
	}

	/**
	 * Specifies if the driver is allowed to read from secondaries or slaves. Defaults to {@literal false}.
	 * 
	 * @param slaveOk true if the driver should read from secondaries or slaves.
	 */
	public void setSlaveOk(boolean slaveOk) {
		this.slaveOk = slaveOk;
	}

	/**
	 * Specifies if the driver should use an SSL connection to Mongo. This defaults to {@literal false}. By default
	 * {@link SSLSocketFactory#getDefault()} will be used. See {@link #setSslSocketFactory(SSLSocketFactory)} if you want
	 * to configure a custom factory.
	 * 
	 * @param ssl true if the driver should use an SSL connection.
	 * @see #setSslSocketFactory(SSLSocketFactory)
	 */
	public void setSsl(boolean ssl) {
		this.ssl = ssl;
	}

	/**
	 * Specifies the {@link SSLSocketFactory} to use for creating SSL connections to Mongo. Defaults to
	 * {@link SSLSocketFactory#getDefault()}. Implicitly activates {@link #setSsl(boolean)} if a non-{@literal null} value
	 * is given.
	 * 
	 * @param sslSocketFactory the sslSocketFactory to use.
	 * @see #setSsl(boolean)
	 */
	public void setSslSocketFactory(SSLSocketFactory sslSocketFactory) {

		setSsl(sslSocketFactory != null);
		this.sslSocketFactory = sslSocketFactory;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	@SuppressWarnings("deprecation")
	public void afterPropertiesSet() {

		MONGO_OPTIONS.connectionsPerHost = connectionsPerHost;
		MONGO_OPTIONS.threadsAllowedToBlockForConnectionMultiplier = threadsAllowedToBlockForConnectionMultiplier;
		MONGO_OPTIONS.maxWaitTime = maxWaitTime;
		MONGO_OPTIONS.connectTimeout = connectTimeout;
		MONGO_OPTIONS.socketTimeout = socketTimeout;
		MONGO_OPTIONS.socketKeepAlive = socketKeepAlive;
		MONGO_OPTIONS.autoConnectRetry = autoConnectRetry;
		MONGO_OPTIONS.maxAutoConnectRetryTime = maxAutoConnectRetryTime;
		MONGO_OPTIONS.slaveOk = slaveOk;
		MONGO_OPTIONS.w = writeNumber;
		MONGO_OPTIONS.wtimeout = writeTimeout;
		MONGO_OPTIONS.fsync = writeFsync;
		if (ssl) {
			MONGO_OPTIONS.setSocketFactory(sslSocketFactory != null ? sslSocketFactory : SSLSocketFactory.getDefault());
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#getObject()
	 */
	public MongoOptions getObject() {
		return MONGO_OPTIONS;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#getObjectType()
	 */
	public Class<?> getObjectType() {
		return MongoOptions.class;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#isSingleton()
	 */
	public boolean isSingleton() {
		return true;
	}
}
