/*
 * Copyright 2023-2024 the original author or authors.
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
package org.springframework.data.mongodb.core.encryption;

import static java.util.Arrays.*;
import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.mongodb.core.EncryptionAlgorithms.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;

import java.security.SecureRandom;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.ClientEncryptionSettings;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.CreateEncryptedCollectionParams;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.client.vault.ClientEncryptions;

import org.assertj.core.api.Assumptions;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.data.convert.PropertyValueConverterFactory;
import org.springframework.data.mongodb.config.AbstractMongoClientConfiguration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions.MongoConverterConfigurationAdapter;
import org.springframework.data.mongodb.core.convert.encryption.MongoEncryptionConverter;
import org.springframework.data.mongodb.core.mapping.ExplicitEncrypted;
import org.springframework.data.mongodb.test.util.EnableIfMongoServerVersion;
import org.springframework.data.mongodb.test.util.EnableIfReplicaSetAvailable;
import org.springframework.data.mongodb.test.util.MongoClientExtension;
import org.springframework.data.mongodb.util.MongoClientVersion;
import org.springframework.data.util.Lazy;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * @author Ross Lawley
 */
@ExtendWith({ MongoClientExtension.class, SpringExtension.class })
@EnableIfMongoServerVersion(isGreaterThanEqual = "8.0")
@EnableIfReplicaSetAvailable
@ContextConfiguration(classes = RangeEncryptionTests.EncryptionConfig.class)
class RangeEncryptionTests {

	@Autowired MongoTemplate template;

	@BeforeEach
	void setUp() {
		Assumptions.assumeThat(MongoClientVersion.isVersion5orNewer()).isTrue();
	}

	@AfterEach
	void tearDown() {
		template.getDb().getCollection("test").deleteMany(new BsonDocument());
	}

	@Test
	void canGreaterThanEqualMatchRangeEncryptedField() {
		Person source = createPerson();
		template.insert(source);

		Person loaded = template.query(Person.class).matching(where("encryptedInt").gte(source.encryptedInt)).firstValue();
		assertThat(loaded).isEqualTo(source);
	}

	@Test
	void canLesserThanEqualMatchRangeEncryptedField() {
		Person source = createPerson();
		template.insert(source);

		Person loaded = template.query(Person.class).matching(where("encryptedInt").lte(source.encryptedInt)).firstValue();
		assertThat(loaded).isEqualTo(source);
	}

	@Test
	void canRangeMatchRangeEncryptedField() {
		Person source = createPerson();
		template.insert(source);

		Person loaded = template.query(Person.class).matching(where("encryptedLong").lte(1001L).gte(1001L)).firstValue();
		assertThat(loaded).isEqualTo(source);
	}

	@Test
	void canUpdateRangeEncryptedField() {
		Person source = createPerson();
		template.insert(source);

		source.encryptedInt = 123;
		source.encryptedLong = 9999L;
		template.save(source);

		Person loaded = template.query(Person.class).matching(where("id").is(source.id)).firstValue();
		assertThat(loaded).isEqualTo(source);
	}

	@Test
	void errorsWhenUsingNonRangeOperatorEqOnRangeEncryptedField() {
		Person source = createPerson();
		template.insert(source);

		assertThatThrownBy(
				() -> template.query(Person.class).matching(where("encryptedInt").is(source.encryptedInt)).firstValue())
				.isInstanceOf(AssertionError.class)
				.hasMessageStartingWith("Not a valid range query. Querying a range encrypted field but "
						+ "the query operator '$eq' for field path 'encryptedInt' is not a range query.");

	}

	@Test
	void errorsWhenUsingNonRangeOperatorInOnRangeEncryptedField() {
		Person source = createPerson();
		template.insert(source);

		assertThatThrownBy(
				() -> template.query(Person.class).matching(where("encryptedLong").in(1001L, 9999L)).firstValue())
				.isInstanceOf(AssertionError.class)
				.hasMessageStartingWith("Not a valid range query. Querying a range encrypted field but "
						+ "the query operator '$in' for field path 'encryptedLong' is not a range query.");

	}

	private Person createPerson() {
		Person source = new Person();
		source.id = "id-1";
		source.encryptedInt = 101;
		source.encryptedLong = 1001L;
		return source;
	}

	protected static class EncryptionConfig extends AbstractMongoClientConfiguration {

		private static final String LOCAL_KMS_PROVIDER = "local";

		private static final Lazy<Map<String, Map<String, Object>>> LAZY_KMS_PROVIDERS = Lazy.of(() -> {
			byte[] localMasterKey = new byte[96];
			new SecureRandom().nextBytes(localMasterKey);
			return Map.of(LOCAL_KMS_PROVIDER, Map.of("key", localMasterKey));
		});

		@Autowired ApplicationContext applicationContext;

		@Override
		protected String getDatabaseName() {
			return "qe-test";
		}

		@Bean
		public MongoClient mongoClient() {
			return super.mongoClient();
		}

		@Override
		protected void configureConverters(MongoConverterConfigurationAdapter converterConfigurationAdapter) {
			converterConfigurationAdapter
					.registerPropertyValueConverterFactory(PropertyValueConverterFactory.beanFactoryAware(applicationContext))
					.useNativeDriverJavaTimeCodecs();
		}

		@Bean
		MongoEncryptionConverter encryptingConverter(MongoClientEncryption mongoClientEncryption) {
			Lazy<Map<String, BsonBinary>> lazyDataKeyMap = Lazy.of(() -> {
				try (MongoClient client = mongoClient()) {
					MongoDatabase database = client.getDatabase(getDatabaseName());
					database.getCollection("test").drop();

					ClientEncryption clientEncryption = mongoClientEncryption.getClientEncryption();
					BsonDocument encryptedFields = new BsonDocument().append("fields",
							new BsonArray(asList(
									new BsonDocument("keyId", BsonNull.VALUE).append("path", new BsonString("encryptedInt"))
											.append("bsonType", new BsonString("int"))
											.append("queries",
													new BsonDocument("queryType", new BsonString("range")).append("contention", new BsonInt64(0L))
															.append("trimFactor", new BsonInt32(1)).append("sparsity", new BsonInt64(1))
															.append("min", new BsonInt32(0)).append("max", new BsonInt32(200))),
									new BsonDocument("keyId", BsonNull.VALUE).append("path", new BsonString("encryptedLong"))
											.append("bsonType", new BsonString("long")).append("queries",
													new BsonDocument("queryType", new BsonString("range")).append("contention", new BsonInt64(0L))
															.append("trimFactor", new BsonInt32(1)).append("sparsity", new BsonInt64(1))
															.append("min", new BsonInt64(1000)).append("max", new BsonInt64(9999))))));

					BsonDocument local = clientEncryption.createEncryptedCollection(database, "test",
							new CreateCollectionOptions().encryptedFields(encryptedFields),
							new CreateEncryptedCollectionParams(LOCAL_KMS_PROVIDER));

					return local.getArray("fields").stream().map(BsonValue::asDocument).collect(
							Collectors.toMap(field -> field.getString("path").getValue(), field -> field.getBinary("keyId")));
				}
			});
			return new MongoEncryptionConverter(mongoClientEncryption, EncryptionKeyResolver
					.annotated((ctx) -> EncryptionKey.keyId(lazyDataKeyMap.get().get(ctx.getProperty().getFieldName()))));
		}

		@Bean
		CachingMongoClientEncryption clientEncryption(ClientEncryptionSettings encryptionSettings) {
			return new CachingMongoClientEncryption(() -> ClientEncryptions.create(encryptionSettings));
		}

		@Override
		protected void configureClientSettings(MongoClientSettings.Builder builder) {
			try (MongoClient client = MongoClients.create()) {
				ClientEncryptionSettings clientEncryptionSettings = encryptionSettings(client);

				builder.autoEncryptionSettings(AutoEncryptionSettings.builder() //
						.kmsProviders(clientEncryptionSettings.getKmsProviders()) //
						.keyVaultNamespace(clientEncryptionSettings.getKeyVaultNamespace()) //
						.bypassQueryAnalysis(true).build());
			}
		}

		@Bean
		ClientEncryptionSettings encryptionSettings(MongoClient mongoClient) {
			MongoNamespace keyVaultNamespace = new MongoNamespace("encryption.testKeyVault");
			MongoCollection<Document> keyVaultCollection = mongoClient.getDatabase(keyVaultNamespace.getDatabaseName())
					.getCollection(keyVaultNamespace.getCollectionName());
			keyVaultCollection.drop();
			// Ensure that two data keys cannot share the same keyAltName.
			keyVaultCollection.createIndex(Indexes.ascending("keyAltNames"),
					new IndexOptions().unique(true).partialFilterExpression(Filters.exists("keyAltNames")));

			mongoClient.getDatabase(getDatabaseName()).getCollection("test").drop(); // Clear old data

			// Create the ClientEncryption instance
			return ClientEncryptionSettings.builder() //
					.keyVaultMongoClientSettings(
							MongoClientSettings.builder().applyConnectionString(new ConnectionString("mongodb://localhost")).build()) //
					.keyVaultNamespace(keyVaultNamespace.getFullName()) //
					.kmsProviders(LAZY_KMS_PROVIDERS.get()) //
					.build();
		}
	}

	static class CachingMongoClientEncryption extends MongoClientEncryption implements DisposableBean {

		static final AtomicReference<ClientEncryption> cache = new AtomicReference<>();

		CachingMongoClientEncryption(Supplier<ClientEncryption> source) {
			super(() -> {
				ClientEncryption clientEncryption = cache.get();
				if (clientEncryption == null) {
					clientEncryption = source.get();
					cache.set(clientEncryption);
				}

				return clientEncryption;
			});
		}

		@Override
		public void destroy() {
			ClientEncryption clientEncryption = cache.get();
			if (clientEncryption != null) {
				clientEncryption.close();
				cache.set(null);
			}
		}
	}

	@org.springframework.data.mongodb.core.mapping.Document("test")
	static class Person {

		String id;
		String name;

		@ExplicitEncrypted(algorithm = RANGE, contentionFactor = 0L,
				rangeOptions = "{\"min\": 0, \"max\": 200, \"trimFactor\": 1, \"sparsity\": 1}") Integer encryptedInt;
		@ExplicitEncrypted(algorithm = RANGE, contentionFactor = 0L,
				rangeOptions = "{\"min\": {\"$numberLong\": \"1000\"}, \"max\": {\"$numberLong\": \"9999\"}, \"trimFactor\": 1, \"sparsity\": 1}") Long encryptedLong;

		public String getId() {
			return this.id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String getName() {
			return this.name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public Integer getEncryptedInt() {
			return this.encryptedInt;
		}

		public void setEncryptedInt(Integer encryptedInt) {
			this.encryptedInt = encryptedInt;
		}

		public Long getEncryptedLong() {
			return this.encryptedLong;
		}

		public void setEncryptedLong(Long encryptedLong) {
			this.encryptedLong = encryptedLong;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o)
				return true;
			if (o == null || getClass() != o.getClass())
				return false;

			Person person = (Person) o;
			return Objects.equals(id, person.id) && Objects.equals(name, person.name)
					&& Objects.equals(encryptedInt, person.encryptedInt) && Objects.equals(encryptedLong, person.encryptedLong);
		}

		@Override
		public int hashCode() {
			int result = Objects.hashCode(id);
			result = 31 * result + Objects.hashCode(name);
			result = 31 * result + Objects.hashCode(encryptedInt);
			result = 31 * result + Objects.hashCode(encryptedLong);
			return result;
		}

		@Override
		public String toString() {
			return "Person{" + "id='" + id + '\'' + ", name='" + name + '\'' + ", encryptedInt=" + encryptedInt
					+ ", encryptedLong=" + encryptedLong + '}';
		}
	}

}
