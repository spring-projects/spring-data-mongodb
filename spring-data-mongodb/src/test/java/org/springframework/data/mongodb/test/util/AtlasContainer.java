/*
 * Copyright 2024 the original author or authors.
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
package org.springframework.data.mongodb.test.util;

import java.util.List;

import org.bson.Document;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.data.util.Lazy;
import org.springframework.util.StringUtils;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.DockerHealthcheckWaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.utility.DockerImageName;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;

/**
 * @author Christoph Strobl
 */
public class AtlasContainer extends GenericContainer<AtlasContainer> {

	private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("mongodb/mongodb-atlas-local");
	private static final String DEFAULT_TAG = "latest";
	private static final String MONGODB_DATABASE_NAME_DEFAULT = "test";
	private static final String READY_DB = "__db_ready_check";
	private final Lazy<MongoClient> client;

	public static AtlasContainer bestMatch() {
		return tagged(new StandardEnvironment().getProperty("mongodb.atlas.version", DEFAULT_TAG));
	}

	public static AtlasContainer latest() {
		return tagged(DEFAULT_TAG);
	}

	public static AtlasContainer version8() {
		return tagged("8.0.0");
	}

	public static AtlasContainer tagged(String tag) {
		return new AtlasContainer(DEFAULT_IMAGE_NAME.withTag(tag));
	}

	public AtlasContainer(String dockerImageName) {
		this(DockerImageName.parse(dockerImageName));
	}

	public AtlasContainer(DockerImageName dockerImageName) {

		super(dockerImageName);
		dockerImageName.assertCompatibleWith(DEFAULT_IMAGE_NAME);
		setExposedPorts(List.of(27017));
		client = Lazy.of(() -> MongoTestUtils.client(new ConnectionString(getConnectionString())));
	}

	public String getConnectionString() {
		return getConnectionString(MONGODB_DATABASE_NAME_DEFAULT);
	}

	/**
	 * Gets a connection string url.
	 *
	 * @return a connection url pointing to a mongodb instance
	 */
	public String getConnectionString(String database) {
		return String.format("mongodb://%s:%d/%s?directConnection=true", getHost(), getMappedPort(27017),
				StringUtils.hasText(database) ? database : MONGODB_DATABASE_NAME_DEFAULT);
	}

	@Override
	public boolean isHealthy() {

		MongoClient mongoClient = client.get();
		MongoCollection<Document> ready = MongoTestUtils.createOrReplaceCollection(READY_DB, "ready", mongoClient);
		boolean isReady = false;

		try {
			ready.aggregate(List.of(new Document("$listSearchIndexes", new Document()))).first();
			isReady = true;
		} catch (Exception e) {
			// ok so the search service is not ready yet - sigh
		}
		if (isReady) {
			mongoClient.getDatabase(READY_DB).drop();
			mongoClient.close();
		}
		return isReady;
	}

	@Override
	protected WaitStrategy getWaitStrategy() {
		return new DockerHealthcheckWaitStrategy();
	}
}
