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

import static org.assertj.core.api.Assertions.assertThat;

import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.data.convert.PropertyValueConverterFactory;
import org.springframework.data.mongodb.config.AbstractMongoClientConfiguration;
import org.springframework.data.mongodb.core.CollectionOptions;
import org.springframework.data.mongodb.core.CollectionOptions.EncryptedFieldsOptions;
import org.springframework.data.mongodb.core.MongoJsonSchemaCreator;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions.MongoConverterConfigurationAdapter;
import org.springframework.data.mongodb.core.convert.encryption.MongoEncryptionConverter;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.RangeEncrypted;
import org.springframework.data.mongodb.core.schema.MongoJsonSchema;
import org.springframework.data.mongodb.test.util.EnableIfMongoServerVersion;
import org.springframework.data.mongodb.test.util.EnableIfReplicaSetAvailable;
import org.springframework.data.mongodb.test.util.MongoClientExtension;
import org.springframework.data.util.Lazy;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

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
import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.client.model.vault.RangeOptions;
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.client.vault.ClientEncryptions;

/**
 * @author Ross Lawley
 * @author Christoph Strobl
 */
@ExtendWith({ MongoClientExtension.class, SpringExtension.class })
@EnableIfMongoServerVersion(isGreaterThanEqual = "8.0")
@EnableIfReplicaSetAvailable
@ContextConfiguration(classes = RangeEncryptionTests.EncryptionConfig.class)
class RangeEncryptionTests {

	@Autowired MongoTemplate template;
	@Autowired MongoClientEncryption clientEncryption;
	@Autowired EncryptionKeyHolder keyHolder;

	@AfterEach
	void tearDown() {
		template.getDb().getCollection("test").deleteMany(new BsonDocument());
	}

	@Test // GH-4185
	void canGreaterThanEqualMatchRangeEncryptedField() {

		EncryptOptions encryptOptions = new EncryptOptions("Range").contentionFactor(1L)
				.keyId(keyHolder.getEncryptionKey("encryptedInt"))
				.rangeOptions(new RangeOptions().min(new BsonInt32(0)).max(new BsonInt32(200)).sparsity(1L));

		EncryptOptions encryptExpressionOptions = new EncryptOptions("Range").contentionFactor(1L)
				.rangeOptions(new RangeOptions().min(new BsonInt32(0)).max(new BsonInt32(200)))
				.keyId(keyHolder.getEncryptionKey("encryptedInt")).queryType("range");

		Document source = new Document("_id", "id-1");
		source.put("name", "It's a Me, Mario!");
		source.put("encryptedInt", clientEncryption.getClientEncryption().encrypt(new BsonInt32(101), encryptOptions));
		source.put("_class", Person.class.getName());

		template.execute(Person.class, col -> col.insertOne(source));

		Document result = template.execute(Person.class, col -> {

			BsonDocument filterSource = new BsonDocument("encryptedInt", new BsonDocument("$gte", new BsonInt32(100)));
			BsonDocument filter = clientEncryption.getClientEncryption()
					.encryptExpression(new Document("$and", List.of(filterSource)), encryptExpressionOptions);

			return col.find(filter).first();
		});

		assertThat(result).containsEntry("encryptedInt", 101);
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
		EncryptionKeyHolder keyHolder(MongoClientEncryption mongoClientEncryption) {

			Lazy<Map<String, BsonBinary>> lazyDataKeyMap = Lazy.of(() -> {
				try (MongoClient client = mongoClient()) {

					MongoDatabase database = client.getDatabase(getDatabaseName());
					database.getCollection("test").drop();

					ClientEncryption clientEncryption = mongoClientEncryption.getClientEncryption();

					MongoJsonSchema personSchema = MongoJsonSchemaCreator.create(new MongoMappingContext()) // init schema creator
							.filter(MongoJsonSchemaCreator.encryptedOnly()) //
							.createSchemaFor(Person.class); //

					Document encryptedFields = CollectionOptions.encryptedCollection(personSchema) //
							.getEncryptedFieldsOptions() //
							.map(EncryptedFieldsOptions::toDocument) //
							.orElseThrow();

					CreateCollectionOptions createCollectionOptions = new CreateCollectionOptions()
							.encryptedFields(encryptedFields);

					BsonDocument local = clientEncryption.createEncryptedCollection(database, "test", createCollectionOptions,
							new CreateEncryptedCollectionParams(LOCAL_KMS_PROVIDER));

					return local.getArray("fields").stream().map(BsonValue::asDocument).collect(
							Collectors.toMap(field -> field.getString("path").getValue(), field -> field.getBinary("keyId")));
				}
			});

			return new EncryptionKeyHolder(lazyDataKeyMap);
		}

		@Bean
		MongoEncryptionConverter encryptingConverter(MongoClientEncryption mongoClientEncryption,
				EncryptionKeyHolder keyHolder) {
			return new MongoEncryptionConverter(mongoClientEncryption, EncryptionKeyResolver
					.annotated((ctx) -> EncryptionKey.keyId(keyHolder.getEncryptionKey(ctx.getProperty().getFieldName()))));
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

	static class EncryptionKeyHolder {

		Supplier<Map<String, BsonBinary>> lazyDataKeyMap;

		public EncryptionKeyHolder(Supplier<Map<String, BsonBinary>> lazyDataKeyMap) {
			this.lazyDataKeyMap = Lazy.of(lazyDataKeyMap);
		}

		BsonBinary getEncryptionKey(String path) {
			return lazyDataKeyMap.get().get(path);
		}
	}

	@org.springframework.data.mongodb.core.mapping.Document("test")
	static class Person {

		String id;
		String name;

		@RangeEncrypted(contentionFactor = 0L,
				rangeOptions = "{\"min\": 0, \"max\": 200, \"trimFactor\": 1, \"sparsity\": 1}") Integer encryptedInt;
		@RangeEncrypted(contentionFactor = 0L,
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
