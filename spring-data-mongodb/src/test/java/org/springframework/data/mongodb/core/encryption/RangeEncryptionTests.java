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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.data.mongodb.core.query.Criteria.where;

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
import org.bson.BsonString;
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
import org.springframework.data.convert.ValueConverter;
import org.springframework.data.mongodb.config.AbstractMongoClientConfiguration;
import org.springframework.data.mongodb.core.CollectionOptions;
import org.springframework.data.mongodb.core.CollectionOptions.EncryptedFieldsOptions;
import org.springframework.data.mongodb.core.MongoJsonSchemaCreator;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions.MongoConverterConfigurationAdapter;
import org.springframework.data.mongodb.core.convert.encryption.MongoEncryptionConverter;
import org.springframework.data.mongodb.core.mapping.Encrypted;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.Queryable;
import org.springframework.data.mongodb.core.mapping.RangeEncrypted;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
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
import com.mongodb.client.result.UpdateResult;
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
	void manuallyEncryptedValuesCanBeSavedAndRetrievedCorrectly() {

		EncryptOptions encryptOptions = new EncryptOptions("Range").contentionFactor(1L)
				.keyId(keyHolder.getEncryptionKey("encryptedInt"))
				.rangeOptions(new RangeOptions().min(new BsonInt32(0)).max(new BsonInt32(200)).sparsity(1L));

		EncryptOptions encryptExpressionOptions = new EncryptOptions("Range").contentionFactor(1L)
				.rangeOptions(new RangeOptions().min(new BsonInt32(0)).max(new BsonInt32(200)))
				.keyId(keyHolder.getEncryptionKey("encryptedInt")).queryType("range");

		EncryptOptions equalityEncOptions = new EncryptOptions("Indexed").contentionFactor(0L)
				.keyId(keyHolder.getEncryptionKey("age"));
		;

		EncryptOptions equalityEncOptionsString = new EncryptOptions("Indexed").contentionFactor(0L)
				.keyId(keyHolder.getEncryptionKey("name"));
		;

		Document source = new Document("_id", "id-1");

		source.put("name",
				clientEncryption.getClientEncryption().encrypt(new BsonString("It's a Me, Mario!"), equalityEncOptionsString));
		source.put("age", clientEncryption.getClientEncryption().encrypt(new BsonInt32(101), equalityEncOptions));
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

	@Test
	void canLesserThanEqualMatchRangeEncryptedField() {

		Person source = createPerson();
		template.insert(source);

		Person loaded = template.query(Person.class).matching(where("encryptedInt").lte(source.encryptedInt)).firstValue();
		assertThat(loaded).isEqualTo(source);
	}

	@Test
	void canQueryMixOfEqualityEncryptedAndUnencrypted() {

		Person source = template.insert(createPerson());

		Person loaded = template.query(Person.class)
				.matching(where("name").is(source.name).and("unencryptedValue").is(source.unencryptedValue)).firstValue();
		assertThat(loaded).isEqualTo(source);
	}

	@Test
	void canQueryMixOfRangeEncryptedAndUnencrypted() {

		Person source = template.insert(createPerson());

		Person loaded = template.query(Person.class)
				.matching(where("encryptedInt").lte(source.encryptedInt).and("unencryptedValue").is(source.unencryptedValue))
				.firstValue();
		assertThat(loaded).isEqualTo(source);
	}

	@Test
	void canQueryEqualityEncryptedField() {

		Person source = createPerson();
		template.insert(source);

		Person loaded = template.query(Person.class).matching(where("age").is(source.age)).firstValue();
		assertThat(loaded).isEqualTo(source);
	}

	@Test
	void canExcludeSafeContentFromResult() {

		Person source = createPerson();
		template.insert(source);

		Query q = Query.query(where("encryptedLong").lte(1001L).gte(1001L));
		q.fields().exclude("__safeContent__");

		Person loaded = template.query(Person.class).matching(q).firstValue();
		assertThat(loaded).isEqualTo(source);
	}

	@Test
	void canRangeMatchRangeEncryptedField() {

		Person source = createPerson();
		template.insert(source);

		Query q = Query.query(where("encryptedLong").lte(1001L).gte(1001L));
		Person loaded = template.query(Person.class).matching(q).firstValue();
		assertThat(loaded).isEqualTo(source);
	}

	@Test
	void canReplaceEntityWithRangeEncryptedField() {

		Person source = createPerson();
		template.insert(source);

		source.encryptedInt = 123;
		source.encryptedLong = 9999L;
		template.save(source);

		Person loaded = template.query(Person.class).matching(where("id").is(source.id)).firstValue();
		assertThat(loaded).isEqualTo(source);
	}

	@Test
	void canUpdateRangeEncryptedField() {

		Person source = createPerson();
		template.insert(source);

		UpdateResult updateResult = template.update(Person.class).matching(where("id").is(source.id))
				.apply(Update.update("encryptedLong", 5000L)).first();
		assertThat(updateResult.getModifiedCount()).isOne();

		Person loaded = template.query(Person.class).matching(where("id").is(source.id)).firstValue();
		assertThat(loaded.encryptedLong).isEqualTo(5000L);
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
		source.unencryptedValue = "y2k";
		source.name = "it's a me mario!";
		source.age = 42;
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

		String unencryptedValue;

		@ValueConverter(MongoEncryptionConverter.class)
		@Encrypted(algorithm = "Indexed") //
		@Queryable(queryType = "equality", contentionFactor = 0) //
		String name;

		@ValueConverter(MongoEncryptionConverter.class)
		// @Encrypted(algorithm = "Indexed", queries = {@Queryable(queryType = "equality", contentionFactor = 0)})
		@Encrypted(algorithm = "Indexed") //
		@Queryable(queryType = "equality", contentionFactor = 0) //
		Integer age;

		@ValueConverter(MongoEncryptionConverter.class)
		@RangeEncrypted(contentionFactor = 0L,
				rangeOptions = "{\"min\": 0, \"max\": 200, \"trimFactor\": 1, \"sparsity\": 1}") //
		Integer encryptedInt;

		@ValueConverter(MongoEncryptionConverter.class)
		@RangeEncrypted(contentionFactor = 0L,
				rangeOptions = "{\"min\": {\"$numberLong\": \"1000\"}, \"max\": {\"$numberLong\": \"9999\"}, \"trimFactor\": 1, \"sparsity\": 1}") //
		Long encryptedLong;

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
			if (o == this) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Person person = (Person) o;
			return Objects.equals(id, person.id) && Objects.equals(unencryptedValue, person.unencryptedValue)
					&& Objects.equals(name, person.name) && Objects.equals(age, person.age)
					&& Objects.equals(encryptedInt, person.encryptedInt) && Objects.equals(encryptedLong, person.encryptedLong);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, unencryptedValue, name, age, encryptedInt, encryptedLong);
		}

		@Override
		public String toString() {
			return "Person{" + "id='" + id + '\'' + ", unencryptedValue='" + unencryptedValue + '\'' + ", name='" + name
					+ '\'' + ", age=" + age + ", encryptedInt=" + encryptedInt + ", encryptedLong=" + encryptedLong + '}';
		}
	}

}
