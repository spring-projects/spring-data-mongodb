/*
 * Copyright 2019 the original author or authors.
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
package org.springframework.data.mongodb.core;

import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;

import org.bson.codecs.configuration.CodecRegistry;
import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.lang.Nullable;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoClientSettings.Builder;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.StreamFactoryFactory;

/**
 * A factory bean for construction of a {@link MongoClientSettings} instance to be used with a MongoDB driver.
 *
 * @author Christoph Strobl
 * @since 3.0
 */
public class MongoClientSettingsFactoryBean extends AbstractFactoryBean<MongoClientSettings> {

	private static final MongoClientSettings DEFAULT_MONGO_SETTINGS = MongoClientSettings.builder().build();

	private CodecRegistry codecRegistry = DEFAULT_MONGO_SETTINGS.getCodecRegistry();
	private StreamFactoryFactory streamFactoryFactory = DEFAULT_MONGO_SETTINGS.getStreamFactoryFactory();

	private ReadPreference readPreference = DEFAULT_MONGO_SETTINGS.getReadPreference();
	private ReadConcern readConcern = DEFAULT_MONGO_SETTINGS.getReadConcern();
	private @Nullable Boolean retryReads = null;

	private WriteConcern writeConcern = DEFAULT_MONGO_SETTINGS.getWriteConcern();
	private @Nullable Boolean retryWrites = null;

	private @Nullable String applicationName = null;

	// --> Socket Settings

	private Integer socketConnectTimeoutMS = DEFAULT_MONGO_SETTINGS.getSocketSettings()
			.getConnectTimeout(TimeUnit.MILLISECONDS);
	private Integer socketReadTimeoutMS = DEFAULT_MONGO_SETTINGS.getSocketSettings()
			.getReadTimeout(TimeUnit.MILLISECONDS);
	private Integer socketReceiveBufferSize = DEFAULT_MONGO_SETTINGS.getSocketSettings().getReceiveBufferSize();
	private Integer socketSendBufferSize = DEFAULT_MONGO_SETTINGS.getSocketSettings().getSendBufferSize();

	/**
	 * @param socketConnectTimeoutMS in msec
	 * @see com.mongodb.connection.SocketSettings.Builder#connectTimeout(int, TimeUnit)
	 */
	public void setSocketConnectTimeoutMS(Integer socketConnectTimeoutMS) {
		this.socketConnectTimeoutMS = socketConnectTimeoutMS;
	}

	/**
	 * @param socketReadTimeoutMS in msec
	 * @see com.mongodb.connection.SocketSettings.Builder#readTimeout(int, TimeUnit)
	 */
	public void setSocketReadTimeoutMS(Integer socketReadTimeoutMS) {
		this.socketReadTimeoutMS = socketReadTimeoutMS;
	}

	/**
	 * @param socketReceiveBufferSize
	 * @see com.mongodb.connection.SocketSettings.Builder#receiveBufferSize(int)
	 */
	public void setSocketReceiveBufferSize(Integer socketReceiveBufferSize) {
		this.socketReceiveBufferSize = socketReceiveBufferSize;
	}

	/**
	 * @param socketSendBufferSize
	 * @see com.mongodb.connection.SocketSettings.Builder#sendBufferSize(int)
	 */
	public void setSocketSendBufferSize(Integer socketSendBufferSize) {
		this.socketSendBufferSize = socketSendBufferSize;
	}

	// --> Server Settings

	private Long serverHeartbeatFrequencyMS = DEFAULT_MONGO_SETTINGS.getServerSettings()
			.getHeartbeatFrequency(TimeUnit.MILLISECONDS);
	private Long serverMinHeartbeatFrequencyMS = DEFAULT_MONGO_SETTINGS.getServerSettings()
			.getMinHeartbeatFrequency(TimeUnit.MILLISECONDS);

	/**
	 * @param serverHeartbeatFrequencyMS in msec
	 * @see com.mongodb.connection.ServerSettings.Builder#heartbeatFrequency(long, TimeUnit)
	 */
	public void setServerHeartbeatFrequencyMS(Long serverHeartbeatFrequencyMS) {
		this.serverHeartbeatFrequencyMS = serverHeartbeatFrequencyMS;
	}

	/**
	 * @param serverMinHeartbeatFrequencyMS in msec
	 * @see com.mongodb.connection.ServerSettings.Builder#minHeartbeatFrequency(long, TimeUnit)
	 */
	public void setServerMinHeartbeatFrequencyMS(Long serverMinHeartbeatFrequencyMS) {
		this.serverMinHeartbeatFrequencyMS = serverMinHeartbeatFrequencyMS;
	}

	// --> Cluster Settings

	private @Nullable String clusterSrvHost = DEFAULT_MONGO_SETTINGS.getClusterSettings().getSrvHost();
	private List<ServerAddress> clusterHosts = Collections.emptyList();
	private @Nullable ClusterConnectionMode clusterConnectionMode = null;
	private ClusterType custerRequiredClusterType = DEFAULT_MONGO_SETTINGS.getClusterSettings().getRequiredClusterType();
	private String clusterRequiredReplicaSetName = DEFAULT_MONGO_SETTINGS.getClusterSettings()
			.getRequiredReplicaSetName();
	private long clusterLocalThresholdMS = DEFAULT_MONGO_SETTINGS.getClusterSettings()
			.getLocalThreshold(TimeUnit.MILLISECONDS);
	private long clusterServerSelectionTimeoutMS = DEFAULT_MONGO_SETTINGS.getClusterSettings()
			.getServerSelectionTimeout(TimeUnit.MILLISECONDS);

	/**
	 * @param clusterSrvHost
	 * @see com.mongodb.connection.ClusterSettings.Builder#srvHost(String)
	 */
	public void setClusterSrvHost(String clusterSrvHost) {
		this.clusterSrvHost = clusterSrvHost;
	}

	/**
	 * @param clusterHosts
	 * @see com.mongodb.connection.ClusterSettings.Builder#hosts(List)
	 */
	public void setClusterHosts(ServerAddress[] clusterHosts) {
		this.clusterHosts = Arrays.asList(clusterHosts);
	}

	/**
	 * ????
	 * 
	 * @param clusterConnectionMode
	 * @see com.mongodb.connection.ClusterSettings.Builder#mode(ClusterConnectionMode)
	 */
	public void setClusterConnectionMode(ClusterConnectionMode clusterConnectionMode) {
		this.clusterConnectionMode = clusterConnectionMode;
	}

	/**
	 * @param custerRequiredClusterType
	 * @see com.mongodb.connection.ClusterSettings.Builder#requiredClusterType(ClusterType)
	 */
	public void setCusterRequiredClusterType(ClusterType custerRequiredClusterType) {
		this.custerRequiredClusterType = custerRequiredClusterType;
	}

	/**
	 * @param clusterRequiredReplicaSetName
	 * @see com.mongodb.connection.ClusterSettings.Builder#requiredReplicaSetName(String)
	 */
	public void setClusterRequiredReplicaSetName(String clusterRequiredReplicaSetName) {
		this.clusterRequiredReplicaSetName = clusterRequiredReplicaSetName;
	}

	/**
	 * @param clusterLocalThresholdMS in msec
	 * @see com.mongodb.connection.ClusterSettings.Builder#localThreshold(long, TimeUnit)
	 */
	public void setClusterLocalThresholdMS(long clusterLocalThresholdMS) {
		this.clusterLocalThresholdMS = clusterLocalThresholdMS;
	}

	/**
	 * @param clusterServerSelectionTimeoutMS in msec
	 * @see com.mongodb.connection.ClusterSettings.Builder#serverSelectionTimeout(long, TimeUnit)
	 */
	public void setClusterServerSelectionTimeoutMS(long clusterServerSelectionTimeoutMS) {
		this.clusterServerSelectionTimeoutMS = clusterServerSelectionTimeoutMS;
	}

	// --> ConnectionPoolSettings

	private Integer poolMaxSize = DEFAULT_MONGO_SETTINGS.getConnectionPoolSettings().getMaxSize();
	private Integer poolMinSize = DEFAULT_MONGO_SETTINGS.getConnectionPoolSettings().getMinSize();
	private Long poolMaxWaitTimeMS = DEFAULT_MONGO_SETTINGS.getConnectionPoolSettings()
			.getMaxWaitTime(TimeUnit.MILLISECONDS);
	private Long poolMaxConnectionLifeTimeMS = DEFAULT_MONGO_SETTINGS.getConnectionPoolSettings()
			.getMaxConnectionLifeTime(TimeUnit.MILLISECONDS);
	private Long poolMaxConnectionIdleTimeMS = DEFAULT_MONGO_SETTINGS.getConnectionPoolSettings()
			.getMaxConnectionIdleTime(TimeUnit.MILLISECONDS);
	private Long poolMaintenanceInitialDelayMS = DEFAULT_MONGO_SETTINGS.getConnectionPoolSettings()
			.getMaintenanceInitialDelay(TimeUnit.MILLISECONDS);
	private Long poolMaintenanceFrequencyMS = DEFAULT_MONGO_SETTINGS.getConnectionPoolSettings()
			.getMaintenanceFrequency(TimeUnit.MILLISECONDS);

	/**
	 * @param poolMaxSize
	 * @see com.mongodb.connection.ConnectionPoolSettings.Builder#maxSize(int)
	 */
	public void setPoolMaxSize(Integer poolMaxSize) {
		this.poolMaxSize = poolMaxSize;
	}

	/**
	 * @param poolMinSize
	 * @see com.mongodb.connection.ConnectionPoolSettings.Builder#minSize(int)
	 */
	public void setPoolMinSize(Integer poolMinSize) {
		this.poolMinSize = poolMinSize;
	}

	/**
	 * @param poolMaxWaitTimeMS in mesec
	 * @see com.mongodb.connection.ConnectionPoolSettings.Builder#maxWaitTime(long, TimeUnit)
	 */
	public void setPoolMaxWaitTimeMS(Long poolMaxWaitTimeMS) {
		this.poolMaxWaitTimeMS = poolMaxWaitTimeMS;
	}

	/**
	 * @param poolMaxConnectionLifeTimeMS in msec
	 * @see com.mongodb.connection.ConnectionPoolSettings.Builder#maxConnectionLifeTime(long, TimeUnit)
	 */
	public void setPoolMaxConnectionLifeTimeMS(Long poolMaxConnectionLifeTimeMS) {
		this.poolMaxConnectionLifeTimeMS = poolMaxConnectionLifeTimeMS;
	}

	/**
	 * @param poolMaxConnectionIdleTimeMS in msec
	 * @see com.mongodb.connection.ConnectionPoolSettings.Builder#maxConnectionIdleTime(long, TimeUnit)
	 */
	public void setPoolMaxConnectionIdleTimeMS(Long poolMaxConnectionIdleTimeMS) {
		this.poolMaxConnectionIdleTimeMS = poolMaxConnectionIdleTimeMS;
	}

	/**
	 * @param poolMaintenanceInitialDelayMS in msec
	 * @see com.mongodb.connection.ConnectionPoolSettings.Builder#maintenanceInitialDelay(long, TimeUnit)
	 */
	public void setPoolMaintenanceInitialDelayMS(Long poolMaintenanceInitialDelayMS) {
		this.poolMaintenanceInitialDelayMS = poolMaintenanceInitialDelayMS;
	}

	/**
	 * @param poolMaintenanceFrequencyMS in msec
	 * @see com.mongodb.connection.ConnectionPoolSettings.Builder#maintenanceFrequency(long, TimeUnit)
	 */
	public void setPoolMaintenanceFrequencyMS(Long poolMaintenanceFrequencyMS) {
		this.poolMaintenanceFrequencyMS = poolMaintenanceFrequencyMS;
	}

	// --> SSL Settings

	private Boolean sslEnabled = DEFAULT_MONGO_SETTINGS.getSslSettings().isEnabled();
	private Boolean sslInvalidHostNameAllowed = DEFAULT_MONGO_SETTINGS.getSslSettings().isInvalidHostNameAllowed();
	private String sslProvider = DEFAULT_MONGO_SETTINGS.getSslSettings().isEnabled()
			? DEFAULT_MONGO_SETTINGS.getSslSettings().getContext().getProvider().getName()
			: "";

	/**
	 * @param sslEnabled
	 * @see com.mongodb.connection.SslSettings.Builder#enabled(boolean)
	 */
	public void setSslEnabled(Boolean sslEnabled) {
		this.sslEnabled = sslEnabled;
	}

	/**
	 * @param sslInvalidHostNameAllowed
	 * @see com.mongodb.connection.SslSettings.Builder#invalidHostNameAllowed(boolean)
	 */
	public void setSslInvalidHostNameAllowed(Boolean sslInvalidHostNameAllowed) {
		this.sslInvalidHostNameAllowed = sslInvalidHostNameAllowed;
	}

	/**
	 * @param sslProvider
	 * @see com.mongodb.connection.SslSettings.Builder#context(SSLContext)
	 * @see SSLContext#getInstance(String)
	 */
	public void setSslProvider(String sslProvider) {
		this.sslProvider = sslProvider;
	}

	// encryption and retry

	private @Nullable AutoEncryptionSettings autoEncryptionSettings;

	/**
	 * @param applicationName
	 * @see MongoClientSettings.Builder#applicationName(String)
	 */
	public void setApplicationName(@Nullable String applicationName) {
		this.applicationName = applicationName;
	}

	/**
	 * @param retryReads
	 * @see MongoClientSettings.Builder#retryReads(boolean)
	 */
	public void setRetryReads(@Nullable Boolean retryReads) {
		this.retryReads = retryReads;
	}

	/**
	 * @param readConcern
	 * @see MongoClientSettings.Builder#readConcern(ReadConcern)
	 */
	public void setReadConcern(ReadConcern readConcern) {
		this.readConcern = readConcern;
	}

	/**
	 * @param writeConcern
	 * @see MongoClientSettings.Builder#writeConcern(WriteConcern)
	 */
	public void setWriteConcern(WriteConcern writeConcern) {
		this.writeConcern = writeConcern;
	}

	/**
	 * @param retryWrites
	 * @see MongoClientSettings.Builder#retryWrites(boolean)
	 */
	public void setRetryWrites(@Nullable Boolean retryWrites) {
		this.retryWrites = retryWrites;
	}

	/**
	 * @param readPreference
	 * @see MongoClientSettings.Builder#readPreference(ReadPreference)
	 */
	public void setReadPreference(ReadPreference readPreference) {
		this.readPreference = readPreference;
	}

	/**
	 * @param streamFactoryFactory
	 * @see MongoClientSettings.Builder#streamFactoryFactory(StreamFactoryFactory)
	 */
	public void setStreamFactoryFactory(StreamFactoryFactory streamFactoryFactory) {
		this.streamFactoryFactory = streamFactoryFactory;
	}

	/**
	 * @param codecRegistry
	 * @see MongoClientSettings.Builder#codecRegistry(CodecRegistry)
	 */
	public void setCodecRegistry(CodecRegistry codecRegistry) {
		this.codecRegistry = codecRegistry;
	}

	/**
	 * @param autoEncryptionSettings can be {@literal null}.
	 * @see MongoClientSettings.Builder#autoEncryptionSettings(AutoEncryptionSettings)
	 */
	public void setAutoEncryptionSettings(@Nullable AutoEncryptionSettings autoEncryptionSettings) {
		this.autoEncryptionSettings = autoEncryptionSettings;
	}

	@Override
	public Class<?> getObjectType() {
		return MongoClientSettings.class;
	}

	@Override
	protected MongoClientSettings createInstance() {

		Builder builder = MongoClientSettings.builder() //
				.readPreference(readPreference) //
				.writeConcern(writeConcern) //
				.readConcern(readConcern) //
				.codecRegistry(codecRegistry) //
				.applicationName(applicationName) //
				.autoEncryptionSettings(autoEncryptionSettings)//
				.applyToClusterSettings((settings) -> {

					settings.serverSelectionTimeout(clusterServerSelectionTimeoutMS, TimeUnit.MILLISECONDS);
					if(clusterConnectionMode != null) {
						settings.mode(clusterConnectionMode);
					}
					settings.requiredReplicaSetName(clusterRequiredReplicaSetName);

					if (!CollectionUtils.isEmpty(clusterHosts)) {
						settings.hosts(clusterHosts);
					}
					settings.localThreshold(clusterLocalThresholdMS, TimeUnit.MILLISECONDS);
//					settings.maxWaitQueueSize(clusterMaxWaitQueueSize);
					settings.requiredClusterType(custerRequiredClusterType);

					if (StringUtils.hasText(clusterSrvHost)) {
						settings.srvHost(clusterSrvHost);
					}
				}) //
				.applyToConnectionPoolSettings((settings) -> {

					settings.minSize(poolMinSize);
					settings.maxSize(poolMaxSize);
					settings.maxConnectionIdleTime(poolMaxConnectionIdleTimeMS, TimeUnit.MILLISECONDS);
					settings.maxWaitTime(poolMaxWaitTimeMS, TimeUnit.MILLISECONDS);
					settings.maxConnectionLifeTime(poolMaxConnectionLifeTimeMS, TimeUnit.MILLISECONDS);
//					settings.maxWaitQueueSize(poolMaxWaitQueueSize);
					settings.maintenanceFrequency(poolMaintenanceFrequencyMS, TimeUnit.MILLISECONDS);
					settings.maintenanceInitialDelay(poolMaintenanceInitialDelayMS, TimeUnit.MILLISECONDS);
				}) //
				.applyToServerSettings((settings) -> {

					settings.minHeartbeatFrequency(serverMinHeartbeatFrequencyMS, TimeUnit.MILLISECONDS);
					settings.heartbeatFrequency(serverHeartbeatFrequencyMS, TimeUnit.MILLISECONDS);
				}) //
				.applyToSocketSettings((settings) -> {

					settings.connectTimeout(socketConnectTimeoutMS.intValue(), TimeUnit.MILLISECONDS);
					settings.readTimeout(socketReadTimeoutMS.intValue(), TimeUnit.MILLISECONDS);
					settings.receiveBufferSize(socketReceiveBufferSize);
					settings.sendBufferSize(socketSendBufferSize);
				}) //
				.applyToSslSettings((settings) -> {

					settings.enabled(sslEnabled);
					if (ObjectUtils.nullSafeEquals(Boolean.TRUE, sslEnabled)) {

						settings.invalidHostNameAllowed(sslInvalidHostNameAllowed);
						try {
							settings.context(StringUtils.hasText(sslProvider) ? SSLContext.getInstance(sslProvider) : SSLContext.getDefault());
						} catch (NoSuchAlgorithmException e) {
							throw new IllegalArgumentException(e.getMessage(), e);
						}
					}
				});

		if (streamFactoryFactory != null) {
			builder = builder.streamFactoryFactory(streamFactoryFactory);
		}
		if (retryReads != null) {
			builder = builder.retryReads(retryReads);
		}
		if (retryWrites != null) {
			builder = builder.retryWrites(retryWrites);
		}

		return builder.build();
	}
}
