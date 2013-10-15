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

	private final MongoOptions MONGO_OPTIONS = new MongoOptions();

	/**
	 * The number of connections allowed per host will block if run out.
	 */
	private int connectionsPerHost = MONGO_OPTIONS.connectionsPerHost;

	/**
	 * A multiplier for connectionsPerHost for # of threads that can block a connection.
	 * <p>
	 * If connectionsPerHost is {@literal 10}, and threadsAllowedToBlockForConnectionMultiplier is {@literal 5}, then
	 * {@literal 50} threads can block. If more threads try to block an exception will be thrown.
	 */
	private int threadsAllowedToBlockForConnectionMultiplier = MONGO_OPTIONS.threadsAllowedToBlockForConnectionMultiplier;

	/**
	 * Max wait time of a blocking thread for a connection.
	 */
	private int maxWaitTime = MONGO_OPTIONS.maxWaitTime;

	/**
	 * Connect timeout in milliseconds. {@literal 0} is default and means infinite time.
	 */
	private int connectTimeout = MONGO_OPTIONS.connectTimeout;

	/**
	 * The socket timeout. {@literal 0} is default and means infinite time.
	 */
	private int socketTimeout = MONGO_OPTIONS.socketTimeout;

	/**
	 * This controls whether or not to have socket keep alive turned on (SO_KEEPALIVE). This defaults to {@literal false}.
	 */
	public boolean socketKeepAlive = MONGO_OPTIONS.socketKeepAlive;

	/**
	 * This controls whether or not the system retries automatically on a failed connect. This defaults to
	 * {@literal false}.
	 */
	private boolean autoConnectRetry = MONGO_OPTIONS.autoConnectRetry;

	private long maxAutoConnectRetryTime = MONGO_OPTIONS.maxAutoConnectRetryTime;

	/**
	 * This specifies the number of servers to wait for on the write operation, and exception raising behavior. This
	 * defaults to {@literal 0}.
	 */
	private int writeNumber;

	/**
	 * This controls timeout for write operations in milliseconds. This defaults to {@literal 0} (indefinite). Greater
	 * than zero is number of milliseconds to wait.
	 */
	private int writeTimeout;

	/**
	 * This controls whether or not to fsync. This defaults to {@literal false}.
	 */
	private boolean writeFsync;

	/**
	 * Specifies if the driver is allowed to read from secondaries or slaves. This defaults to {@literal false}.
	 */
	@SuppressWarnings("deprecation") private boolean slaveOk = MONGO_OPTIONS.slaveOk;

	/**
	 * This controls SSL support via SSLSocketFactory. This defaults to {@literal false}.
	 */
	private boolean ssl;

	/**
	 * Specifies the {@link SSLSocketFactory} to use. This defaults to {@link SSLSocketFactory#getDefault()}
	 */
	private SSLSocketFactory sslSocketFactory;

	/**
	 * The maximum number of connections allowed per host until we will block.
	 * 
	 * @param connectionsPerHost
	 */
	public void setConnectionsPerHost(int connectionsPerHost) {
		this.connectionsPerHost = connectionsPerHost;
	}

	/**
	 * A multiplier for connectionsPerHost for # of threads that can block a connection.
	 * 
	 * @see #threadsAllowedToBlockForConnectionMultiplier
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
	 * The connect timeout in milliseconds. {@literal 0} is default and infinite
	 * 
	 * @param connectTimeout
	 */
	public void setConnectTimeout(int connectTimeout) {
		this.connectTimeout = connectTimeout;
	}

	/**
	 * The socket timeout. {@literal 0} is default and infinite.
	 * 
	 * @param socketTimeout
	 */
	public void setSocketTimeout(int socketTimeout) {
		this.socketTimeout = socketTimeout;
	}

	/**
	 * This controls whether or not to have socket keep alive.
	 * 
	 * @param socketKeepAlive
	 */
	public void setSocketKeepAlive(boolean socketKeepAlive) {
		this.socketKeepAlive = socketKeepAlive;
	}

	/**
	 * This specifies the number of servers to wait for on the write operation, and exception raising behavior. The 'w'
	 * option to the getlasterror command. Defaults to {@literal 0}.
	 * <ul>
	 * <li>{@literal -1} = don't even report network errors</li>
	 * <li>{@literal 0} = default, don't call getLastError by default</li>
	 * <li>{@literal 1} = basic, call getLastError, but don't wait for slaves</li>
	 * <li>{@literal 2} += wait for slaves</li>
	 * </ul>
	 * 
	 * @param writeNumber the number of servers to wait for on the write operation, and exception raising behavior.
	 */
	public void setWriteNumber(int writeNumber) {
		this.writeNumber = writeNumber;
	}

	/**
	 * This controls timeout for write operations in milliseconds. The 'wtimeout' option to the getlasterror command.
	 * 
	 * @param writeTimeout Defaults to {@literal 0} (indefinite). Greater than zero is number of milliseconds to wait.
	 */
	public void setWriteTimeout(int writeTimeout) {
		this.writeTimeout = writeTimeout;
	}

	/**
	 * This controls whether or not to fsync. The 'fsync' option to the getlasterror command. Defaults to {@literal false}
	 * 
	 * @param writeFsync to fsync on <code>write (true)<code>, otherwise {@literal false}.
	 */
	public void setWriteFsync(boolean writeFsync) {
		this.writeFsync = writeFsync;
	}

	/**
	 * Controls whether or not the system retries automatically, on a failed connect.
	 * 
	 * @param autoConnectRetry
	 */
	public void setAutoConnectRetry(boolean autoConnectRetry) {
		this.autoConnectRetry = autoConnectRetry;
	}

	/**
	 * The maximum amount of time in millisecons to spend retrying to open connection to the same server. This defaults to
	 * {@literal 0}, which means to use the default {@literal 15s} if {@link #autoConnectRetry} is on.
	 * 
	 * @param maxAutoConnectRetryTime the maxAutoConnectRetryTime to set
	 */
	public void setMaxAutoConnectRetryTime(long maxAutoConnectRetryTime) {
		this.maxAutoConnectRetryTime = maxAutoConnectRetryTime;
	}

	/**
	 * Specifies if the driver is allowed to read from secondaries or slaves. This defaults to {@literal false}.
	 * 
	 * @param slaveOk true if the driver should read from secondaries or slaves.
	 */
	public void setSlaveOk(boolean slaveOk) {
		this.slaveOk = slaveOk;
	}

	/**
	 * Specifies if the driver should use an SSL connection to Mongo. This defaults to {@literal false}.
	 * 
	 * @param ssl true if the driver should use an SSL connection.
	 */
	public void setSsl(boolean ssl) {
		this.ssl = ssl;
	}

	/**
	 * Specifies the SSLSocketFactory to use for creating SSL connections to Mongo.
	 * 
	 * @param sslSocketFactory the sslSocketFactory to use.
	 */
	public void setSslSocketFactory(SSLSocketFactory sslSocketFactory) {

		setSsl(sslSocketFactory != null);
		this.sslSocketFactory = sslSocketFactory;
	}

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

	public MongoOptions getObject() {
		return MONGO_OPTIONS;
	}

	public Class<?> getObjectType() {
		return MongoOptions.class;
	}

	public boolean isSingleton() {
		return true;
	}
}
