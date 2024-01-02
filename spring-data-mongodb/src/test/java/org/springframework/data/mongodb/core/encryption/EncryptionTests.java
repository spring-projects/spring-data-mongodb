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

import static org.springframework.data.mongodb.core.EncryptionAlgorithms.*;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.security.SecureRandom;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.bson.BsonBinary;
import org.bson.Document;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.convert.PropertyValueConverterFactory;
import org.springframework.data.mongodb.config.AbstractMongoClientConfiguration;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions.MongoConverterConfigurationAdapter;
import org.springframework.data.mongodb.core.convert.encryption.MongoEncryptionConverter;
import org.springframework.data.mongodb.core.encryption.EncryptionTests.Config;
import org.springframework.data.mongodb.core.mapping.ExplicitEncrypted;
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
 * @author Julia Lee
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = Config.class)
public class EncryptionTests extends AbstractEncryptionTestBase {

	@Configuration
	static class Config extends AbstractMongoClientConfiguration {

		@Autowired ApplicationContext applicationContext;

		@Override
		protected String getDatabaseName() {
			return "fle-test";
		}

		@Bean
		@Override
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

			Lazy<BsonBinary> dataKey = Lazy.of(() -> mongoClientEncryption.getClientEncryption().createDataKey("local",
					new DataKeyOptions().keyAltNames(Collections.singletonList("mySuperSecretKey"))));

			return new MongoEncryptionConverter(mongoClientEncryption,
					EncryptionKeyResolver.annotated((ctx) -> EncryptionKey.keyId(dataKey.get())));
		}

		@Bean
		CachingMongoClientEncryption clientEncryption(ClientEncryptionSettings encryptionSettings) {
			return new CachingMongoClientEncryption(() -> ClientEncryptions.create(encryptionSettings));
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

			byte[] localMasterKey = new byte[96];
			new SecureRandom().nextBytes(localMasterKey);
			Map<String, Map<String, Object>> kmsProviders = Map.of("local", Map.of("key", localMasterKey));

			// Create the ClientEncryption instance
			return ClientEncryptionSettings.builder()
					.keyVaultMongoClientSettings(
							MongoClientSettings.builder().applyConnectionString(new ConnectionString("mongodb://localhost")).build()) //
					.keyVaultNamespace(keyVaultNamespace.getFullName()) //
					.kmsProviders(kmsProviders) //
					.build();
		}

	}

	@Data
	@org.springframework.data.mongodb.core.mapping.Document("test")
	static class Person {

		String id;
		String name;

		@ExplicitEncrypted(algorithm = AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic) //
		String ssn;

		@ExplicitEncrypted(algorithm = AEAD_AES_256_CBC_HMAC_SHA_512_Random, keyAltName = "mySuperSecretKey") //
		String wallet;

		@ExplicitEncrypted(algorithm = AEAD_AES_256_CBC_HMAC_SHA_512_Random) // full document must be random
		Address address;

		AddressWithEncryptedZip encryptedZip;

		@ExplicitEncrypted(algorithm = AEAD_AES_256_CBC_HMAC_SHA_512_Random) // lists must be random
		List<String> listOfString;

		@ExplicitEncrypted(algorithm = AEAD_AES_256_CBC_HMAC_SHA_512_Random) // lists must be random
		List<Address> listOfComplex;

		@ExplicitEncrypted(algorithm = AEAD_AES_256_CBC_HMAC_SHA_512_Random, keyAltName = "/name") //
		String viaAltKeyNameField;

		@ExplicitEncrypted(algorithm = AEAD_AES_256_CBC_HMAC_SHA_512_Random) //
		Map<String, String> mapOfString;

		@ExplicitEncrypted(algorithm = AEAD_AES_256_CBC_HMAC_SHA_512_Random) //
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

		@ExplicitEncrypted(algorithm = AEAD_AES_256_CBC_HMAC_SHA_512_Random) String zip;

		@Override
		public String toString() {
			return "AddressWithEncryptedZip{" + "zip='" + zip + '\'' + ", city='" + getCity() + '\'' + ", street='"
					+ getStreet() + '\'' + '}';
		}
	}
}
