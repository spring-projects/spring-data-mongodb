/*
 * Copyright 2023 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.mongodb.core.EncryptionAlgorithms.*;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import org.assertj.core.api.Assertions;
import org.bson.BsonBinary;
import org.bson.Document;
import org.bson.types.Binary;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.PermissionDeniedDataAccessException;
import org.springframework.data.convert.PropertyValueConverterFactory;
import org.springframework.data.mongodb.config.AbstractMongoClientConfiguration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions.MongoConverterConfigurationAdapter;
import org.springframework.data.mongodb.core.convert.encryption.MongoEncryptionConverter;
import org.springframework.data.mongodb.core.encryption.EncryptionTests.Config;
import org.springframework.data.mongodb.core.mapping.ExplicitlyEncrypted;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.util.Lazy;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.mongodb.ClientEncryptionSettings;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.vault.ClientEncryptions;

/**
 * @author Christoph Strobl
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = Config.class)
public class EncryptionTests {

	@Autowired MongoTemplate template;

	@Test // GH-4284
	void enDeCryptSimpleValue() {

		Person source = new Person();
		source.id = "id-1";
		source.ssn = "mySecretSSN";

		template.save(source);

		verifyThat(source) //
				.identifiedBy(Person::getId) //
				.wasSavedMatching(it -> assertThat(it.get("ssn")).isInstanceOf(Binary.class)) //
				.loadedIsEqualToSource();
	}

	@Test // GH-4284
	void enDeCryptComplexValue() {

		Person source = new Person();
		source.id = "id-1";
		source.address = new Address();
		source.address.city = "NYC";
		source.address.street = "4th Ave.";

		template.save(source);

		verifyThat(source) //
				.identifiedBy(Person::getId) //
				.wasSavedMatching(it -> assertThat(it.get("address")).isInstanceOf(Binary.class)) //
				.loadedIsEqualToSource();
	}

	@Test // GH-4284
	void enDeCryptValueWithinComplexOne() {

		Person source = new Person();
		source.id = "id-1";
		source.encryptedZip = new AddressWithEncryptedZip();
		source.encryptedZip.city = "Boston";
		source.encryptedZip.street = "central square";
		source.encryptedZip.zip = "1234567890";

		template.save(source);

		verifyThat(source) //
				.identifiedBy(Person::getId) //
				.wasSavedMatching(it -> {
					assertThat(it.get("encryptedZip")).isInstanceOf(Document.class);
					assertThat(it.get("encryptedZip", Document.class).get("city")).isInstanceOf(String.class);
					assertThat(it.get("encryptedZip", Document.class).get("street")).isInstanceOf(String.class);
					assertThat(it.get("encryptedZip", Document.class).get("zip")).isInstanceOf(Binary.class);
				}) //
				.loadedIsEqualToSource();
	}

	@Test // GH-4284
	void enDeCryptListOfSimpleValue() {

		Person source = new Person();
		source.id = "id-1";
		source.listOfString = Arrays.asList("spring", "data", "mongodb");

		template.save(source);

		verifyThat(source) //
				.identifiedBy(Person::getId) //
				.wasSavedMatching(it -> assertThat(it.get("listOfString")).isInstanceOf(Binary.class)) //
				.loadedIsEqualToSource();
	}

	@Test // GH-4284
	void enDeCryptListOfComplexValue() {

		Person source = new Person();
		source.id = "id-1";

		Address address = new Address();
		address.city = "SFO";
		address.street = "---";

		source.listOfComplex = Collections.singletonList(address);

		template.save(source);

		verifyThat(source) //
				.identifiedBy(Person::getId) //
				.wasSavedMatching(it -> assertThat(it.get("listOfComplex")).isInstanceOf(Binary.class)) //
				.loadedIsEqualToSource();
	}

	@Test // GH-4284
	void enDeCryptMapOfSimpleValues() {

		Person source = new Person();
		source.id = "id-1";
		source.mapOfString = Map.of("k1", "v1", "k2", "v2");

		template.save(source);

		verifyThat(source) //
				.identifiedBy(Person::getId) //
				.wasSavedMatching(it -> assertThat(it.get("mapOfString")).isInstanceOf(Binary.class)) //
				.loadedIsEqualToSource();
	}

	@Test // GH-4284
	void enDeCryptMapOfComplexValues() {

		Person source = new Person();
		source.id = "id-1";

		Address address1 = new Address();
		address1.city = "SFO";
		address1.street = "---";

		Address address2 = new Address();
		address2.city = "NYC";
		address2.street = "---";

		source.mapOfComplex = Map.of("a1", address1, "a2", address2);

		template.save(source);

		verifyThat(source) //
				.identifiedBy(Person::getId) //
				.wasSavedMatching(it -> assertThat(it.get("mapOfComplex")).isInstanceOf(Binary.class)) //
				.loadedIsEqualToSource();
	}

	@Test // GH-4284
	void canQueryDeterministicallyEncrypted() {

		Person source = new Person();
		source.id = "id-1";
		source.ssn = "mySecretSSN";

		template.save(source);

		Person loaded = template.query(Person.class).matching(where("ssn").is(source.ssn)).firstValue();
		assertThat(loaded).isEqualTo(source);
	}

	@Test // GH-4284
	void cannotQueryRandomlyEncrypted() {

		Person source = new Person();
		source.id = "id-1";
		source.wallet = "secret-wallet-id";

		template.save(source);

		Person loaded = template.query(Person.class).matching(where("wallet").is(source.wallet)).firstValue();
		assertThat(loaded).isNull();
	}

	@Test // GH-4284
	void updateSimpleTypeEncryptedFieldWithNewValue() {

		Person source = new Person();
		source.id = "id-1";

		template.save(source);

		template.update(Person.class).matching(where("id").is(source.id)).apply(Update.update("ssn", "secret-value"))
				.first();

		verifyThat(source) //
				.identifiedBy(Person::getId) //
				.wasSavedMatching(it -> assertThat(it.get("ssn")).isInstanceOf(Binary.class)) //
				.loadedMatches(it -> assertThat(it.getSsn()).isEqualTo("secret-value"));
	}

	@Test // GH-4284
	void updateComplexTypeEncryptedFieldWithNewValue() {

		Person source = new Person();
		source.id = "id-1";

		template.save(source);

		Address address = new Address();
		address.city = "SFO";
		address.street = "---";

		template.update(Person.class).matching(where("id").is(source.id)).apply(Update.update("address", address)).first();

		verifyThat(source) //
				.identifiedBy(Person::getId) //
				.wasSavedMatching(it -> assertThat(it.get("address")).isInstanceOf(Binary.class)) //
				.loadedMatches(it -> assertThat(it.getAddress()).isEqualTo(address));
	}

	@Test // GH-4284
	void updateEncryptedFieldInNestedElementWithNewValue() {

		Person source = new Person();
		source.id = "id-1";
		source.encryptedZip = new AddressWithEncryptedZip();
		source.encryptedZip.city = "Boston";
		source.encryptedZip.street = "central square";

		template.save(source);

		template.update(Person.class).matching(where("id").is(source.id)).apply(Update.update("encryptedZip.zip", "179"))
				.first();

		verifyThat(source) //
				.identifiedBy(Person::getId) //
				.wasSavedMatching(it -> {
					assertThat(it.get("encryptedZip")).isInstanceOf(Document.class);
					assertThat(it.get("encryptedZip", Document.class).get("city")).isInstanceOf(String.class);
					assertThat(it.get("encryptedZip", Document.class).get("street")).isInstanceOf(String.class);
					assertThat(it.get("encryptedZip", Document.class).get("zip")).isInstanceOf(Binary.class);
				}) //
				.loadedMatches(it -> assertThat(it.getEncryptedZip().getZip()).isEqualTo("179"));
	}

	@Test
	void aggregationWithMatch() {

		Person person = new Person();
		person.id = "id-1";
		person.name = "p1-name";
		person.ssn = "mySecretSSN";

		template.save(person);

		AggregationResults<Person> aggregationResults = template.aggregateAndReturn(Person.class)
				.by(newAggregation(Person.class, Aggregation.match(where("ssn").is(person.ssn)))).all();
		assertThat(aggregationResults.getMappedResults()).containsExactly(person);
	}

	@Test
	void altKeyDetection(@Autowired MongoClientEncryption mongoClientEncryption) throws InterruptedException {

		BsonBinary user1key = mongoClientEncryption.getClientEncryption().createDataKey("local",
				new DataKeyOptions().keyAltNames(Collections.singletonList("user-1")));

		BsonBinary user2key = mongoClientEncryption.getClientEncryption().createDataKey("local",
				new DataKeyOptions().keyAltNames(Collections.singletonList("user-2")));

		Person p1 = new Person();
		p1.id = "id-1";
		p1.name = "user-1";
		p1.ssn = "ssn";
		p1.viaAltKeyNameField = "value-1";

		Person p2 = new Person();
		p2.id = "id-2";
		p2.name = "user-2";
		p2.viaAltKeyNameField = "value-1";

		Person p3 = new Person();
		p3.id = "id-3";
		p3.name = "user-1";
		p3.viaAltKeyNameField = "value-1";

		template.save(p1);
		template.save(p2);
		template.save(p3);

		template.execute(Person.class, collection -> {
			collection.find(new Document()).forEach(it -> System.out.println(it.toJson()));
			return null;
		});

		// remove the key and invalidate encrypted data
		mongoClientEncryption.getClientEncryption().deleteKey(user2key);

		// clear the 60 second key cache within the mongo client
		mongoClientEncryption.refresh();

		assertThat(template.query(Person.class).matching(where("id").is(p1.id)).firstValue()).isEqualTo(p1);

		assertThatExceptionOfType(PermissionDeniedDataAccessException.class)
				.isThrownBy(() -> template.query(Person.class).matching(where("id").is(p2.id)).firstValue());
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
			return wasSavedMatching(it -> Assertions.assertThat(it).isEqualTo(expected));
		}

		SaveAndLoadAssert<T> wasSavedMatching(Consumer<Document> saved) {
			EncryptionTests.this.assertSaved(source, idProvider, saved);
			return this;
		}

		SaveAndLoadAssert<T> loadedMatches(Consumer<T> expected) {
			EncryptionTests.this.assertLoaded(source, idProvider, expected);
			return this;
		}

		SaveAndLoadAssert<T> loadedIsEqualToSource() {
			return loadedIsEqualTo(source);
		}

		SaveAndLoadAssert<T> loadedIsEqualTo(T expected) {
			return loadedMatches(it -> Assertions.assertThat(it).isEqualTo(expected));
		}

	}

	<T> void assertSaved(T source, Function<T, ?> idProvider, Consumer<Document> dbValue) {

		Document savedDocument = template.execute(Person.class, collection -> {
			return collection.find(new Document("_id", idProvider.apply(source))).first();
		});
		dbValue.accept(savedDocument);
	}

	<T> void assertLoaded(T source, Function<T, ?> idProvider, Consumer<T> loadedValue) {

		T loaded = template.query((Class<T>) source.getClass()).matching(where("id").is(idProvider.apply(source)))
				.firstValue();

		loadedValue.accept(loaded);
	}

	@Configuration
	static class Config extends AbstractMongoClientConfiguration {

		@Autowired ApplicationContext applicationContext;

		@Override
		protected String getDatabaseName() {
			return "fle-test";
		}

		@Bean
		public MongoClient mongoClient() {
			return super.mongoClient();
		}

		@Override
		protected void configureConverters(MongoConverterConfigurationAdapter converterConfigurationAdapter) {

			converterConfigurationAdapter
					.registerPropertyValueConverterFactory(PropertyValueConverterFactory.beanFactoryAware(applicationContext));
		}

		@Bean
		MongoEncryptionConverter encryptingConverter(MongoClientEncryption mongoClientEncryption) {

			Lazy<BsonBinary> dataKey = Lazy.of(() -> mongoClientEncryption.getClientEncryption().createDataKey("local",
					new DataKeyOptions().keyAltNames(Collections.singletonList("mySuperSecretKey"))));

			return new MongoEncryptionConverter(mongoClientEncryption,
					EncryptionKeyResolver.annotationBased((ctx) -> EncryptionKey.keyId(dataKey.get())));
		}

		@Bean
		MongoClientEncryption clientEncryption(ClientEncryptionSettings encryptionSettings) {
			return MongoClientEncryption.caching(() -> ClientEncryptions.create(encryptionSettings));
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

			MongoCollection<Document> collection = mongoClient.getDatabase(getDatabaseName()).getCollection("test");
			collection.drop(); // Clear old data

			final byte[] localMasterKey = new byte[96];
			new SecureRandom().nextBytes(localMasterKey);
			Map<String, Map<String, Object>> kmsProviders = new HashMap<String, Map<String, Object>>() {
				{
					put("local", new HashMap<String, Object>() {
						{
							put("key", localMasterKey);
						}
					});
				}
			};

			// Create the ClientEncryption instance
			ClientEncryptionSettings clientEncryptionSettings = ClientEncryptionSettings.builder()
					.keyVaultMongoClientSettings(
							MongoClientSettings.builder().applyConnectionString(new ConnectionString("mongodb://localhost")).build())
					.keyVaultNamespace(keyVaultNamespace.getFullName()).kmsProviders(kmsProviders).build();
			return clientEncryptionSettings;
		}
	}

	@Data
	@org.springframework.data.mongodb.core.mapping.Document("test")
	static class Person {

		String id;
		String name;

		@ExplicitlyEncrypted(algorithm = AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic) //
		String ssn;

		@ExplicitlyEncrypted(algorithm = AEAD_AES_256_CBC_HMAC_SHA_512_Random, altKeyName = "mySuperSecretKey") //
		String wallet;

		@ExplicitlyEncrypted(algorithm = AEAD_AES_256_CBC_HMAC_SHA_512_Random) // full document must be random
		Address address;

		AddressWithEncryptedZip encryptedZip;

		@ExplicitlyEncrypted(algorithm = AEAD_AES_256_CBC_HMAC_SHA_512_Random) // lists must be random
		List<String> listOfString;

		@ExplicitlyEncrypted(algorithm = AEAD_AES_256_CBC_HMAC_SHA_512_Random) // lists must be random
		List<Address> listOfComplex;

		@ExplicitlyEncrypted(algorithm = AEAD_AES_256_CBC_HMAC_SHA_512_Random, altKeyName = "/name") //
		String viaAltKeyNameField;

		@ExplicitlyEncrypted(algorithm = AEAD_AES_256_CBC_HMAC_SHA_512_Random) //
		Map<String, String> mapOfString;

		@ExplicitlyEncrypted(algorithm = AEAD_AES_256_CBC_HMAC_SHA_512_Random) //
		Map<String, Address> mapOfComplex;
	}

	@Data
	static class Address {
		String city;
		String street;
	}

	@Getter
	@Setter
	static class AddressWithEncryptedZip extends Address {

		@ExplicitlyEncrypted(algorithm = AEAD_AES_256_CBC_HMAC_SHA_512_Random) String zip;

		@Override
		public String toString() {
			return "AddressWithEncryptedZip{" + "zip='" + zip + '\'' + ", city='" + getCity() + '\'' + ", street='"
					+ getStreet() + '\'' + '}';
		}
	}
}
