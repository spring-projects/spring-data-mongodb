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

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.lang.Nullable;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoClientSettings.Builder;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.ServerSettings;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.event.ClusterListener;

/**
 * Convenient factory for configuring MongoDB.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class MongoClientFactoryBean extends AbstractFactoryBean<MongoClient> implements PersistenceExceptionTranslator {

	private static final PersistenceExceptionTranslator DEFAULT_EXCEPTION_TRANSLATOR = new MongoExceptionTranslator();

	private @Nullable MongoClientSettings mongoClientSettings;
	private @Nullable String host;
	private @Nullable Integer port;
	private @Nullable List<MongoCredential> credential = null;
	private @Nullable ConnectionString connectionString;
	private @Nullable String replicaSet = null;

	private PersistenceExceptionTranslator exceptionTranslator = DEFAULT_EXCEPTION_TRANSLATOR;

	/**
	 * Set the {@link MongoClientSettings} to be used when creating {@link MongoClient}.
	 *
	 * @param mongoClientOptions
	 */
	public void setMongoClientSettings(@Nullable MongoClientSettings mongoClientOptions) {
		this.mongoClientSettings = mongoClientOptions;
	}

	/**
	 * Set the list of credentials to be used when creating {@link MongoClient}.
	 *
	 * @param credential can be {@literal null}.
	 */
	public void setCredential(@Nullable MongoCredential[] credential) {
		this.credential = Arrays.asList(credential);
	}

	/**
	 * Configures the host to connect to.
	 *
	 * @param host
	 */
	public void setHost(@Nullable String host) {
		this.host = host;
	}

	/**
	 * Configures the port to connect to.
	 *
	 * @param port
	 */
	public void setPort(int port) {
		this.port = port;
	}

	public void setConnectionString(@Nullable ConnectionString connectionString) {
		this.connectionString = connectionString;
	}

	public void setReplicaSet(@Nullable String replicaSet) {
		this.replicaSet = replicaSet;
	}

	/**
	 * Configures the {@link PersistenceExceptionTranslator} to use.
	 *
	 * @param exceptionTranslator
	 */
	public void setExceptionTranslator(@Nullable PersistenceExceptionTranslator exceptionTranslator) {
		this.exceptionTranslator = exceptionTranslator == null ? DEFAULT_EXCEPTION_TRANSLATOR : exceptionTranslator;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#getObjectType()
	 */
	public Class<? extends MongoClient> getObjectType() {
		return MongoClient.class;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.dao.support.PersistenceExceptionTranslator#translateExceptionIfPossible(java.lang.RuntimeException)
	 */
	@Nullable
	public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
		return exceptionTranslator.translateExceptionIfPossible(ex);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.config.AbstractFactoryBean#createInstance()
	 */
	@Override
	protected MongoClient createInstance() throws Exception {
		return createMongoClient(computeClientSetting());
	}

	/**
	 * Create {@link MongoClientSettings} based on configuration and priority (lower is better). <br />
	 * 1. {@link MongoClientFactoryBean#mongoClientSettings} <br />
	 * 2. {@link MongoClientFactoryBean#connectionString} <br />
	 * 3. default {@link MongoClientSettings}
	 * 
	 * @since 3.0
	 */
	protected MongoClientSettings computeClientSetting() {

		if (connectionString != null && (StringUtils.hasText(host) || port != null)) {
			throw new IllegalStateException("ConnectionString and host/port configuration exclude one another!");
		}

		ConnectionString connectionString = this.connectionString != null ? this.connectionString
				: new ConnectionString(String.format("mongodb://%s:%s", getOrDefault(host, ServerAddress.defaultHost()),
						getOrDefault(port, "" + ServerAddress.defaultPort())));

		Builder builder = MongoClientSettings.builder().applyConnectionString(connectionString);

		if (mongoClientSettings != null) {

			MongoClientSettings defaultSettings = MongoClientSettings.builder().build();

			SslSettings sslSettings = mongoClientSettings.getSslSettings();
			ClusterSettings clusterSettings = mongoClientSettings.getClusterSettings();
			ConnectionPoolSettings connectionPoolSettings = mongoClientSettings.getConnectionPoolSettings();
			SocketSettings socketSettings = mongoClientSettings.getSocketSettings();
			ServerSettings serverSettings = mongoClientSettings.getServerSettings();

			builder = builder //
					.applicationName(computeSettingsValue(defaultSettings.getApplicationName(),
							mongoClientSettings.getApplicationName(), connectionString.getApplicationName())) //
					.applyToSslSettings(settings -> {

						applySettings(settings::enabled, computeSettingsValue(SslSettings::isEnabled,
								defaultSettings.getSslSettings(), sslSettings, connectionString.getSslEnabled()));
						applySettings(settings::invalidHostNameAllowed, (computeSettingsValue(SslSettings::isInvalidHostNameAllowed,
								defaultSettings.getSslSettings(), sslSettings, connectionString.getSslInvalidHostnameAllowed())));
						settings.context(sslSettings.getContext());
					}).applyToClusterSettings(settings -> {

						applySettings(settings::hosts,
								computeSettingsValue(ClusterSettings::getHosts, defaultSettings.getClusterSettings(), clusterSettings,
										connectionString.getHosts().stream().map(ServerAddress::new).collect(Collectors.toList())));

						applySettings(settings::requiredReplicaSetName,
								computeSettingsValue(ClusterSettings::getRequiredReplicaSetName, defaultSettings.getClusterSettings(),
										clusterSettings, connectionString.getRequiredReplicaSetName()));

						applySettings(settings::srvHost, computeSettingsValue(ClusterSettings::getSrvHost,
								defaultSettings.getClusterSettings(), clusterSettings, null));

						applySettings(settings::mode, computeSettingsValue(ClusterSettings::getMode,
								defaultSettings.getClusterSettings(), clusterSettings, null));

						applySettings(it -> settings.localThreshold(it.longValue(), TimeUnit.MILLISECONDS),
								computeSettingsValue((ClusterSettings it) -> it.getLocalThreshold(TimeUnit.MILLISECONDS),
										defaultSettings.getClusterSettings(), clusterSettings, connectionString.getLocalThreshold()));

						applySettings(settings::requiredClusterType, computeSettingsValue(ClusterSettings::getRequiredClusterType,
								defaultSettings.getClusterSettings(), clusterSettings, null));
						applySettings(it -> settings.serverSelectionTimeout(it.longValue(), TimeUnit.MILLISECONDS),
								computeSettingsValue((ClusterSettings it) -> it.getServerSelectionTimeout(TimeUnit.MILLISECONDS),
										defaultSettings.getClusterSettings(), clusterSettings,
										connectionString.getServerSelectionTimeout()));

						applySettings(settings::serverSelector, computeSettingsValue(ClusterSettings::getServerSelector,
								defaultSettings.getClusterSettings(), clusterSettings, null));
						List<ClusterListener> clusterListeners = computeSettingsValue(ClusterSettings::getClusterListeners,
								defaultSettings.getClusterSettings(), clusterSettings, null);
						if (clusterListeners != null) {
							clusterListeners.forEach(settings::addClusterListener);
						}
					}) //
					.applyToConnectionPoolSettings(settings -> {

						applySettings(it -> settings.maintenanceFrequency(it.longValue(), TimeUnit.MILLISECONDS),
								computeSettingsValue((ConnectionPoolSettings it) -> it.getMaintenanceFrequency(TimeUnit.MILLISECONDS),
										defaultSettings.getConnectionPoolSettings(), connectionPoolSettings, null));

						applySettings(it -> settings.maxConnectionIdleTime(it.longValue(), TimeUnit.MILLISECONDS),
								computeSettingsValue((ConnectionPoolSettings it) -> it.getMaxConnectionIdleTime(TimeUnit.MILLISECONDS),
										defaultSettings.getConnectionPoolSettings(), connectionPoolSettings,
										connectionString.getMaxConnectionIdleTime()));

						applySettings(it -> settings.maxConnectionLifeTime(it.longValue(), TimeUnit.MILLISECONDS),
								computeSettingsValue((ConnectionPoolSettings it) -> it.getMaxConnectionLifeTime(TimeUnit.MILLISECONDS),
										defaultSettings.getConnectionPoolSettings(), connectionPoolSettings,
										connectionString.getMaxConnectionLifeTime()));

						applySettings(it -> settings.maxWaitTime(it.longValue(), TimeUnit.MILLISECONDS),
								computeSettingsValue((ConnectionPoolSettings it) -> it.getMaxWaitTime(TimeUnit.MILLISECONDS),
										defaultSettings.getConnectionPoolSettings(), connectionPoolSettings,
										connectionString.getMaxWaitTime()));

						applySettings(it -> settings.maintenanceInitialDelay(it.longValue(), TimeUnit.MILLISECONDS),
								computeSettingsValue(
										(ConnectionPoolSettings it) -> it.getMaintenanceInitialDelay(TimeUnit.MILLISECONDS),
										defaultSettings.getConnectionPoolSettings(), connectionPoolSettings, null));

						applySettings(settings::minSize,
								computeSettingsValue(ConnectionPoolSettings::getMinSize, defaultSettings.getConnectionPoolSettings(),
										connectionPoolSettings, connectionString.getMinConnectionPoolSize()));
						applySettings(settings::maxSize,
								computeSettingsValue(ConnectionPoolSettings::getMaxSize, defaultSettings.getConnectionPoolSettings(),
										connectionPoolSettings, connectionString.getMaxConnectionPoolSize()));
					}) //
					.applyToSocketSettings(settings -> {

						applySettings(it -> settings.connectTimeout(it.intValue(), TimeUnit.MILLISECONDS),
								computeSettingsValue((SocketSettings it) -> it.getConnectTimeout(TimeUnit.MILLISECONDS),
										defaultSettings.getSocketSettings(), socketSettings, connectionString.getConnectTimeout()));

						applySettings(it -> settings.readTimeout(it.intValue(), TimeUnit.MILLISECONDS),
								computeSettingsValue((SocketSettings it) -> it.getReadTimeout(TimeUnit.MILLISECONDS),
										defaultSettings.getSocketSettings(), socketSettings, connectionString.getSocketTimeout()));
						applySettings(settings::receiveBufferSize, computeSettingsValue(SocketSettings::getReceiveBufferSize,
								defaultSettings.getSocketSettings(), socketSettings, null));
						applySettings(settings::sendBufferSize, computeSettingsValue(SocketSettings::getSendBufferSize,
								defaultSettings.getSocketSettings(), socketSettings, null));
					}) //
					.applyToServerSettings(settings -> {

						applySettings(it -> settings.minHeartbeatFrequency(it.intValue(), TimeUnit.MILLISECONDS),
								computeSettingsValue((ServerSettings it) -> it.getMinHeartbeatFrequency(TimeUnit.MILLISECONDS),
										defaultSettings.getServerSettings(), serverSettings, null));

						applySettings(it -> settings.heartbeatFrequency(it.intValue(), TimeUnit.MILLISECONDS),
								computeSettingsValue((ServerSettings it) -> it.getHeartbeatFrequency(TimeUnit.MILLISECONDS),
										defaultSettings.getServerSettings(), serverSettings, connectionString.getHeartbeatFrequency()));
						settings.applySettings(serverSettings);
					}) //
					.autoEncryptionSettings(mongoClientSettings.getAutoEncryptionSettings()) //
					.codecRegistry(mongoClientSettings.getCodecRegistry()); //

			applySettings(builder::readConcern, computeSettingsValue(defaultSettings.getReadConcern(),
					mongoClientSettings.getReadConcern(), connectionString.getReadConcern()));
			applySettings(builder::writeConcern, computeSettingsValue(defaultSettings.getWriteConcern(),
					mongoClientSettings.getWriteConcern(), connectionString.getWriteConcern()));
			applySettings(builder::readPreference, computeSettingsValue(defaultSettings.getReadPreference(),
					mongoClientSettings.getReadPreference(), connectionString.getReadPreference()));
			applySettings(builder::retryReads, computeSettingsValue(defaultSettings.getRetryReads(),
					mongoClientSettings.getRetryReads(), connectionString.getRetryReads()));
			applySettings(builder::retryWrites, computeSettingsValue(defaultSettings.getRetryWrites(),
					mongoClientSettings.getRetryWrites(), connectionString.getRetryWritesValue()));
		}

		if (!CollectionUtils.isEmpty(credential)) {
			builder = builder.credential(credential.iterator().next());
		}

		if (StringUtils.hasText(replicaSet)) {
			builder.applyToClusterSettings((settings) -> {
				settings.requiredReplicaSetName(replicaSet);
			});
		}

		return builder.build();
	}

	private <T> void applySettings(Consumer<T> settingsBuilder, @Nullable T value) {

		if (ObjectUtils.isEmpty(value)) {
			return;
		}
		settingsBuilder.accept(value);
	}

	private <S, T> T computeSettingsValue(Function<S, T> function, S defaultValueHolder, S settingsValueHolder,
			@Nullable T connectionStringValue) {
		return computeSettingsValue(function.apply(defaultValueHolder), function.apply(settingsValueHolder),
				connectionStringValue);
	}

	private <T> T computeSettingsValue(T defaultValue, T fromSettings, T fromConnectionString) {

		boolean fromSettingsIsDefault = ObjectUtils.nullSafeEquals(defaultValue, fromSettings);
		boolean fromConnectionStringIsDefault = ObjectUtils.nullSafeEquals(defaultValue, fromConnectionString);

		if (!fromSettingsIsDefault) {
			return fromSettings;
		}
		return !fromConnectionStringIsDefault ? fromConnectionString : defaultValue;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.config.AbstractFactoryBean#destroyInstance(java.lang.Object)
	 */
	@Override
	protected void destroyInstance(@Nullable MongoClient instance) throws Exception {

		if (instance != null) {
			instance.close();
		}
	}

	private MongoClient createMongoClient(MongoClientSettings settings) throws UnknownHostException {
		return MongoClients.create(settings);
	}

	private String getOrDefault(Object value, String defaultValue) {
		return !StringUtils.isEmpty(value) ? value.toString() : defaultValue;
	}
}
