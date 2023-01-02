/*
 * Copyright 2013-2023 the original author or authors.
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
package org.springframework.data.mongodb.config;

import static org.springframework.data.mongodb.test.util.Assertions.*;

import java.util.Collections;
import java.util.Set;

import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataAccessException;
import org.springframework.data.mongodb.core.CollectionCallback;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.test.util.MongoClientClosingTestConfiguration;
import org.springframework.data.mongodb.test.util.MongoTestUtils;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;

/**
 * @author Oliver Gierke
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration
public abstract class AbstractIntegrationTests {

	@Configuration
	static class TestConfig extends MongoClientClosingTestConfiguration {

		@Override
		protected String getDatabaseName() {
			return "database";
		}

		@Override
		public MongoClient mongoClient() {
			return MongoTestUtils.client();
		}

		@Override
		protected Set<Class<?>> getInitialEntitySet() throws ClassNotFoundException {
			return Collections.emptySet();
		}

		@Override
		protected boolean autoIndexCreation() {
			return true;
		}
	}

	@Autowired MongoOperations operations;

	@BeforeEach
	@AfterEach
	public void cleanUp() {

		for (String collectionName : operations.getCollectionNames()) {
			if (!collectionName.startsWith("system")) {
				operations.execute(collectionName, new CollectionCallback<Void>() {
					@Override
					public Void doInCollection(MongoCollection<Document> collection) throws MongoException, DataAccessException {
						collection.deleteMany(new Document());
						assertThat(collection.find().iterator().hasNext()).isFalse();
						return null;
					}
				});
			}
		}
	}
}
