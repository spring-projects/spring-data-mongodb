/*
 * Copyright 2016 the original author or authors.
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

import java.util.ArrayList;
import java.util.List;

import org.bson.codecs.configuration.CodecRegistry;
import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.util.Assert;

import com.mongodb.MongoCredential;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.async.client.MongoClientSettings;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.ServerSettings;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.connection.StreamFactoryFactory;

/**
 * A factory bean for construction of a {@link MongoClientSettings} instance to be used with the async MongoDB driver.
 *
 * @author Mark Paluch
 * @since 2.0
 */
public class ReactiveMongoClientSettingsFactoryBean extends AbstractFactoryBean<MongoClientSettings> {

	private static final MongoClientSettings DEFAULT_MONGO_SETTINGS = MongoClientSettings.builder().build();

	private ReadPreference readPreference = DEFAULT_MONGO_SETTINGS.getReadPreference();
	private WriteConcern writeConcern = DEFAULT_MONGO_SETTINGS.getWriteConcern();
	private ReadConcern readConcern = DEFAULT_MONGO_SETTINGS.getReadConcern();
	private List<MongoCredential> credentialList = new ArrayList<>();
	private StreamFactoryFactory streamFactoryFactory = DEFAULT_MONGO_SETTINGS.getStreamFactoryFactory();
	private CodecRegistry codecRegistry = DEFAULT_MONGO_SETTINGS.getCodecRegistry();
	private ClusterSettings clusterSettings = DEFAULT_MONGO_SETTINGS.getClusterSettings();
	private SocketSettings socketSettings = DEFAULT_MONGO_SETTINGS.getSocketSettings();
	private SocketSettings heartbeatSocketSettings = DEFAULT_MONGO_SETTINGS.getHeartbeatSocketSettings();
	private ConnectionPoolSettings connectionPoolSettings = DEFAULT_MONGO_SETTINGS.getConnectionPoolSettings();
	private ServerSettings serverSettings = DEFAULT_MONGO_SETTINGS.getServerSettings();
	private SslSettings sslSettings = DEFAULT_MONGO_SETTINGS.getSslSettings();

	/**
	 * Set the {@link ReadPreference}.
	 *
	 * @param readPreference
	 */
	public void setReadPreference(ReadPreference readPreference) {
		this.readPreference = readPreference;
	}

	/**
	 * Set the {@link WriteConcern}.
	 *
	 * @param writeConcern
	 */
	public void setWriteConcern(WriteConcern writeConcern) {
		this.writeConcern = writeConcern;
	}

	/**
	 * Set the {@link ReadConcern}.
	 *
	 * @param readConcern
	 */
	public void setReadConcern(ReadConcern readConcern) {
		this.readConcern = readConcern;
	}

	/**
	 * Set the List of {@link MongoCredential}s.
	 *
	 * @param credentialList must not be {@literal null}.
	 */
	public void setCredentialList(List<MongoCredential> credentialList) {

		Assert.notNull(credentialList, "CredendialList must not be null!");

		this.credentialList.addAll(credentialList);
	}

	/**
	 * Adds the {@link MongoCredential} to the list of credentials.
	 *
	 * @param mongoCredential must not be {@literal null}.
	 */
	public void addMongoCredential(MongoCredential mongoCredential) {

		Assert.notNull(mongoCredential, "MongoCredential must not be null!");

		this.credentialList.add(mongoCredential);
	}

	/**
	 * Set the {@link StreamFactoryFactory}.
	 *
	 * @param streamFactoryFactory
	 */
	public void setStreamFactoryFactory(StreamFactoryFactory streamFactoryFactory) {
		this.streamFactoryFactory = streamFactoryFactory;
	}

	/**
	 * Set the {@link CodecRegistry}.
	 *
	 * @param codecRegistry
	 */
	public void setCodecRegistry(CodecRegistry codecRegistry) {
		this.codecRegistry = codecRegistry;
	}

	/**
	 * Set the {@link ClusterSettings}.
	 *
	 * @param clusterSettings
	 */
	public void setClusterSettings(ClusterSettings clusterSettings) {
		this.clusterSettings = clusterSettings;
	}

	/**
	 * Set the {@link SocketSettings}.
	 *
	 * @param socketSettings
	 */
	public void setSocketSettings(SocketSettings socketSettings) {
		this.socketSettings = socketSettings;
	}

	/**
	 * Set the heartbeat {@link SocketSettings}.
	 *
	 * @param heartbeatSocketSettings
	 */
	public void setHeartbeatSocketSettings(SocketSettings heartbeatSocketSettings) {
		this.heartbeatSocketSettings = heartbeatSocketSettings;
	}

	/**
	 * Set the {@link ConnectionPoolSettings}.
	 *
	 * @param connectionPoolSettings
	 */
	public void setConnectionPoolSettings(ConnectionPoolSettings connectionPoolSettings) {
		this.connectionPoolSettings = connectionPoolSettings;
	}

	/**
	 * Set the {@link ServerSettings}.
	 *
	 * @param serverSettings
	 */
	public void setServerSettings(ServerSettings serverSettings) {
		this.serverSettings = serverSettings;
	}

	/**
	 * Set the {@link SslSettings}.
	 *
	 * @param sslSettings
	 */
	public void setSslSettings(SslSettings sslSettings) {
		this.sslSettings = sslSettings;
	}

	@Override
	public Class<?> getObjectType() {
		return MongoClientSettings.class;
	}

	@Override
	protected MongoClientSettings createInstance() throws Exception {

		return MongoClientSettings.builder() //
				.readPreference(readPreference) //
				.writeConcern(writeConcern) //
				.readConcern(readConcern) //
				.credentialList(credentialList) //
				.streamFactoryFactory(streamFactoryFactory) //
				.codecRegistry(codecRegistry) //
				.clusterSettings(clusterSettings) //
				.socketSettings(socketSettings) //
				.heartbeatSocketSettings(heartbeatSocketSettings) //
				.connectionPoolSettings(connectionPoolSettings) //
				.serverSettings(serverSettings) //
				.sslSettings(sslSettings) //
				.build();
	}
}
