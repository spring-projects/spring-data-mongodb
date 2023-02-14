/*
 * Copyright 2022 the original author or authors.
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
package org.springframework.data.mongodb.fle;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.mongodb.core.EncryptionAlgorithms.*;
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
import org.springframework.data.mongodb.core.convert.MongoCustomConversions.MongoConverterConfigurationAdapter;
import org.springframework.data.mongodb.core.encryption.ClientEncryptionConverter;
import org.springframework.data.mongodb.core.encryption.ClientEncryptionProvider;
import org.springframework.data.mongodb.core.encryption.EncryptionKey;
import org.springframework.data.mongodb.core.encryption.EncryptionKeyProvider;
import org.springframework.data.mongodb.core.encryption.ExplicitlyEncrypted;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.fle.FLETests.Config;
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
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.vault.ClientEncryptions;

/**
 * @author Christoph Strobl
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = Config.class)
public class FLETests {

	@Autowired MongoTemplate template;

	@Test
	void manualEnAndDecryption() {

		Person person = new Person();
		person.id = "id-1";
		person.name = "p1-name";
		person.ssn = "mySecretSSN"; // determinisitc encryption (queryable)
		person.wallet = "myEvenMoreSecretStuff"; // random encryption (non queryable)

		// nested full document encryption
		person.address = new Address();
		person.address.city = "NYC";
		person.address.street = "4th Ave.";

		person.encryptedZip = new AddressWithEncryptedZip();
		person.encryptedZip.city = "Boston";
		person.encryptedZip.street = "central square";
		person.encryptedZip.zip = "1234567890";

		person.listOfString = Arrays.asList("spring", "data", "mongodb");

		Address partOfList = new Address();
		partOfList.city = "SFO";
		partOfList.street = "---";
		person.listOfComplex = Collections.singletonList(partOfList);

		template.save(person);

		System.out.println("source: " + person);

		Document savedDocument = template.execute(Person.class, collection -> {
			return collection.find(new Document()).first();
		});

		// ssn should look like "ssn": {"$binary": {"base64": "...
		System.out.println("saved: " + savedDocument.toJson());
		assertThat(savedDocument.get("ssn")).isInstanceOf(Binary.class);
		assertThat(savedDocument.get("wallet")).isInstanceOf(Binary.class);
		assertThat(savedDocument.get("encryptedZip")).isInstanceOf(Document.class);
		assertThat(savedDocument.get("encryptedZip", Document.class).get("zip")).isInstanceOf(Binary.class);
		assertThat(savedDocument.get("address")).isInstanceOf(Binary.class);
		assertThat(savedDocument.get("listOfString")).isInstanceOf(Binary.class);
		assertThat(savedDocument.get("listOfComplex")).isInstanceOf(Binary.class);

		// count should be 1 using a deterministic algorithm
		long queryCount = template.query(Person.class).matching(where("ssn").is(person.ssn)).count();
		System.out.println("query(count): " + queryCount);
		assertThat(queryCount).isOne();

		Person bySsn = template.query(Person.class).matching(where("ssn").is(person.ssn)).firstValue();
		System.out.println("queryable: " + bySsn);
		assertThat(bySsn).isEqualTo(person);

		Person byWallet = template.query(Person.class).matching(where("wallet").is(person.wallet)).firstValue();
		System.out.println("not-queryable: " + byWallet);
		assertThat(byWallet).isNull();
	}

	@Test
	void theUpdateStuff() {

		Person person = new Person();
		person.id = "id-1";
		person.name = "p1-name";

		template.save(person);

		Document savedDocument = template.execute(Person.class, collection -> {
			return collection.find(new Document()).first();
		});
		System.out.println("saved: " + savedDocument.toJson());

		template.update(Person.class).matching(where("id").is(person.id)).apply(Update.update("ssn", "secret-value"))
				.first();

		savedDocument = template.execute(Person.class, collection -> {
			return collection.find(new Document()).first();
		});
		System.out.println("updated: " + savedDocument.toJson());
		assertThat(savedDocument.get("ssn")).isInstanceOf(Binary.class);

	}

	@Test
	// @Disabled("for now - takes to long ")
	void altKeyDetection(@Autowired ClientEncryptionProvider clientEncryptionProvider) throws InterruptedException {

		BsonBinary user1key = clientEncryptionProvider.getClientEncryption().createDataKey("local",
				new DataKeyOptions().keyAltNames(Collections.singletonList("user-1")));

		BsonBinary user2key = clientEncryptionProvider.getClientEncryption().createDataKey("local",
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

		// System.out.println(template.query(Person.class).matching(where("id").is(p1.id)).firstValue());
		// System.out.println(template.query(Person.class).matching(where("id").is(p2.id)).firstValue());

		DeleteResult deleteResult = clientEncryptionProvider.getClientEncryption().deleteKey(user2key);
		clientEncryptionProvider.getClientEncryption().getKeys().forEach(System.out::println);
		System.out.println("deleteResult: " + deleteResult);

		// System.out.println("---- waiting for cache timeout ----");
		// TimeUnit.SECONDS.sleep(90);
		clientEncryptionProvider.refresh();

		assertThat(template.query(Person.class).matching(where("id").is(p1.id)).firstValue()).isEqualTo(p1);

		assertThatExceptionOfType(PermissionDeniedDataAccessException.class)
				.isThrownBy(() -> template.query(Person.class).matching(where("id").is(p2.id)).firstValue());
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
		ClientEncryptionConverter encryptingConverter(ClientEncryptionProvider clientEncryptionProvider) {

			Lazy<BsonBinary> dataKey = Lazy.of(() -> clientEncryptionProvider.getClientEncryption().createDataKey("local",
					new DataKeyOptions().keyAltNames(Collections.singletonList("mySuperSecretKey"))));

			return new ClientEncryptionConverter(clientEncryptionProvider, EncryptionKeyProvider.annotationBasedKeyProvider(() -> EncryptionKey.keyId(dataKey.get())));
		}

		@Bean
		ClientEncryptionProvider clientEncryption(ClientEncryptionSettings encryptionSettings) {
			return ClientEncryptionProvider.caching(() -> ClientEncryptions.create(encryptionSettings));
		}

		@Bean
		ClientEncryptionSettings encryptionSettings(MongoClient mongoClient) {

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

			MongoNamespace keyVaultNamespace = new MongoNamespace("encryption.testKeyVault");
			MongoCollection<Document> keyVaultCollection = mongoClient.getDatabase(keyVaultNamespace.getDatabaseName())
					.getCollection(keyVaultNamespace.getCollectionName());
			keyVaultCollection.drop();
			// Ensure that two data keys cannot share the same keyAltName.
			keyVaultCollection.createIndex(Indexes.ascending("keyAltNames"),
					new IndexOptions().unique(true).partialFilterExpression(Filters.exists("keyAltNames")));

			MongoCollection<Document> collection = mongoClient.getDatabase(getDatabaseName()).getCollection("test");
			collection.drop(); // Clear old data

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
