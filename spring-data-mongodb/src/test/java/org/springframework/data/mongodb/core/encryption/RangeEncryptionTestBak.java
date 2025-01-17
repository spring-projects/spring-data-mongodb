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
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.types.Binary;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.data.convert.PropertyValueConverterFactory;
import org.springframework.data.mongodb.config.AbstractMongoClientConfiguration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions.MongoConverterConfigurationAdapter;
import org.springframework.data.mongodb.core.convert.encryption.MongoEncryptionConverter;
import org.springframework.data.mongodb.core.mapping.ExplicitEncrypted;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.test.util.MongoClientExtension;
import org.springframework.data.util.Lazy;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.security.SecureRandom;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.mongodb.core.EncryptionAlgorithms.RANGE;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.newAggregation;
import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * @author Ross Lawley
 */
@ExtendWith(MongoClientExtension.class)
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = RangeEncryptionTestBak.EncryptionConfig.class)
public class RangeEncryptionTestBak {

	@Autowired MongoTemplate template;
    // TODO
	/*

	Todo:

- [X] Add {{encryptedFields}} support to {{CreateCollectionOptions}}
- [X] Add {{contentionFactor}} to {{EncryptOptions}}
- [X] Add {{queryType}} to {{EncryptOptions}}
- [X] Add {{RangeOptions}} to {{EncryptOptions}}
- [ ] Add {{rangeOptions}} (String / JSON) to {{ExplicitEncrypted}} annotation
- [ ] Add {{Range}} and {{Indexed}} to encryption algorithms.
- [ ] Add {{encryptExpression}} support to {{EncryptingConverter}}
- [ ] Add test cases from the [Test Plan

// TODO - add support for Indexed

		Test Plan

		Setup:
		  - Create a POJO with the valid range bson data types, annotate the fields with @ExplicitEncrypted.
		  - Insert test data
		  - Validate the data has been encrypted in the db.

		Single range tests:
		  - Perform a Range query for each of the encrypted fields
		  - Validate the expected POJO(s) is turned

		Multiple field range tests:
		  - Perform a Range query on multiple the encrypted fields at once
		  - Validate the expected POJO(s) is turned

		Multiple field tests:
		  - Perform a Range query on an encrypted fields as well as a non encrypted field
		  - Validate the expected POJO(s) is turned
	 */

	@Test
	void canEqualityMatchRangeEncryptedField() {
		System.out.println("START");
		Person source = new Person();
		source.id = "id-1";
		source.sid = 111;

		System.out.println("SAVE");
		template.save(source);
		System.out.println("QUERY");
		Person loaded = template.query(Person.class).matching(where("sid").is(source.sid)).firstValue();
		assertThat(loaded).isEqualTo(source);
	}

	@Test
	void updateSimpleTypeEncryptedFieldWithNewValue() {

		Person source = new Person();
		source.id = "id-1";

		template.save(source);

		template.update(Person.class).matching(where("id").is(source.id)).apply(Update.update("sid", 123))
				.first();

		verifyThat(source) //
				.identifiedBy(Person::getId) //
				.wasSavedMatching(it -> assertThat(it.get("ssn")).isInstanceOf(Binary.class)) //
				.loadedMatches(it -> assertThat(it.getSid()).isEqualTo(123));
	}

	@Test
	void aggregationWithMatch() {

		Person person = new Person();
		person.id = "id-1";
		person.name = "p1-name";
		person.sid =  321;

		template.save(person);

		AggregationResults<Person> aggregationResults = template.aggregateAndReturn(Person.class)
				.by(newAggregation(Person.class, Aggregation.match(where("ssn").is(person.sid)))).all();
		assertThat(aggregationResults.getMappedResults()).containsExactly(person);
	}


	<T> SaveAndLoadAssert<T> verifyThat(T source) {
		return new SaveAndLoadAssert<>(source);
	}

	class SaveAndLoadAssert<T> {

		T source;
		Function<T, ?> idProvider;

		SaveAndLoadAssert(T source) {
			this.source = source;
		}

		SaveAndLoadAssert<T> identifiedBy(Function<T, ?> idProvider) {
			this.idProvider = idProvider;
			return this;
		}

		SaveAndLoadAssert<T> wasSavedAs(Document expected) {
			return wasSavedMatching(it -> assertThat(it).isEqualTo(expected));
		}

		SaveAndLoadAssert<T> wasSavedMatching(Consumer<Document> saved) {
			RangeEncryptionTestBak.this.assertSaved(source, idProvider, saved);
			return this;
		}

		SaveAndLoadAssert<T> loadedMatches(Consumer<T> expected) {
			RangeEncryptionTestBak.this.assertLoaded(source, idProvider, expected);
			return this;
		}

		SaveAndLoadAssert<T> loadedIsEqualToSource() {
			return loadedIsEqualTo(source);
		}

		SaveAndLoadAssert<T> loadedIsEqualTo(T expected) {
			return loadedMatches(it -> assertThat(it).isEqualTo(expected));
		}

	}

	<T> void assertSaved(T source, Function<T, ?> idProvider, Consumer<Document> dbValue) {

		Document savedDocument = template.execute(Person.class, collection -> {

			MongoNamespace namespace = collection.getNamespace();

			try (MongoClient rawClient = MongoClients.create()) {
				return rawClient.getDatabase(namespace.getDatabaseName()).getCollection(namespace.getCollectionName())
						.find(new Document("_id", idProvider.apply(source))).first();
			}
		});
		dbValue.accept(savedDocument);
	}

	<T> void assertLoaded(T source, Function<T, ?> idProvider, Consumer<T> loadedValue) {

		T loaded = template.query((Class<T>) source.getClass()).matching(where("id").is(idProvider.apply(source)))
				.firstValue();

		loadedValue.accept(loaded);
	}

	protected static class  EncryptionConfig extends AbstractMongoClientConfiguration {

		@Autowired ApplicationContext applicationContext;

		@Override
		protected String getDatabaseName() {
			return "qe-test";
		}

		protected String getCollectionName() {
			return "test";
		}

		@Bean
		public MongoClient mongoClient() {
			return super.mongoClient();
		}

		@Override
		protected void configureClientSettings(MongoClientSettings.Builder builder) {
			try (MongoClient mongoClient = MongoClients.create()) {
				ClientEncryptionSettings clientEncryptionSettings = encryptionSettings();

				MongoNamespace keyVaultNamespace = new MongoNamespace("encryption.testKeyVault");
				MongoCollection<Document> keyVaultCollection = mongoClient.getDatabase(keyVaultNamespace.getDatabaseName())
						.getCollection(keyVaultNamespace.getCollectionName());
				keyVaultCollection.drop();

				// Ensure that two data keys cannot share the same keyAltName.
				keyVaultCollection.createIndex(Indexes.ascending("keyAltNames"),
						new IndexOptions().unique(true).partialFilterExpression(Filters.exists("keyAltNames")));

				builder.autoEncryptionSettings(AutoEncryptionSettings.builder() //
						.kmsProviders(clientEncryptionSettings.getKmsProviders()) //
						.keyVaultNamespace(clientEncryptionSettings.getKeyVaultNamespace()) //
						.bypassAutoEncryption(true)
						.build());
			}
		}

		@Override
		protected void configureConverters(MongoConverterConfigurationAdapter converterConfigurationAdapter) {
			converterConfigurationAdapter
					.registerPropertyValueConverterFactory(PropertyValueConverterFactory.beanFactoryAware(applicationContext))
					.useNativeDriverJavaTimeCodecs();
		}

		@Bean
		MongoEncryptionConverter encryptingConverter(MongoClientEncryption mongoClientEncryption) {
			Lazy<BsonBinary> lazyDataKey = Lazy.of(() -> {

				BsonDocument encryptedFields = new BsonDocument()
						.append(
								"fields",
								new BsonArray(singletonList(new BsonDocument("keyId", BsonNull.VALUE)
										.append("path", new BsonString("sid"))
										.append("bsonType", new BsonString("int"))
										.append(
												"queries",
												new BsonDocument("queryType", new BsonString("range"))
														.append("contention", new BsonInt64(0L))
														.append("trimFactor", new BsonInt32(1))
														.append("sparsity", new BsonInt64(1))
														.append("min", new BsonInt32(0))
														.append("max", new BsonInt32(200))))));

				try (MongoClient client = mongoClient()) {
					MongoDatabase database = client.getDatabase(getDatabaseName());
					database.getCollection(getCollectionName()).drop();
					BsonDocument local = mongoClientEncryption.getClientEncryption()
							.createEncryptedCollection(database, getCollectionName(),
									new CreateCollectionOptions().encryptedFields(encryptedFields),
									new CreateEncryptedCollectionParams("local"));
					return local.getArray("fields").get(0).asDocument().getBinary("keyId");
				}


			});
			return new MongoEncryptionConverter(mongoClientEncryption,
					EncryptionKeyResolver.annotated((ctx) -> EncryptionKey.keyId(lazyDataKey.get())));
		}

		@Bean
		CachingMongoClientEncryption clientEncryption(ClientEncryptionSettings encryptionSettings) {
			return new CachingMongoClientEncryption(() -> ClientEncryptions.create(encryptionSettings));
		}

		@Bean
		ClientEncryptionSettings encryptionSettings() {
			MongoNamespace keyVaultNamespace = new MongoNamespace("encryption.testKeyVault");

			byte[] localMasterKey = new byte[96];
			new SecureRandom().nextBytes(localMasterKey);
			Map<String, Map<String, Object>> kmsProviders = Map.of("local", Map.of("key", localMasterKey));

			// Create the ClientEncryption instance
			return ClientEncryptionSettings.builder() //
					.keyVaultMongoClientSettings(
							MongoClientSettings.builder().applyConnectionString(new ConnectionString("mongodb://localhost")).build()) //
					.keyVaultNamespace(keyVaultNamespace.getFullName()) //
					.kmsProviders(kmsProviders) //
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

		@ExplicitEncrypted(algorithm = RANGE, contentionFactor = 0L, rangeOptions = "{min: 0, max: 200, trimFactor: 1, sparsity: 1}")
//		@ExplicitEncrypted(algorithm = AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic)
		Integer sid;


		public String getId() {
			return this.id;
		}

		public String getName() {
			return this.name;
		}

		public Integer getSid() {
			return this.sid;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setName(String name) {
			this.name = name;
		}

		public void setSid(Integer sid) {
			this.sid = sid;
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
			return Objects.equals(id, person.id) && Objects.equals(name, person.name) && Objects.equals(sid, person.sid);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, name, sid);
		}

		public String toString() {
			return "EncryptionTests.Person(id=" + this.getId() + ", name=" + this.getName() + ", sid=" + this.getSid()+ ")";
		}
	}
}
