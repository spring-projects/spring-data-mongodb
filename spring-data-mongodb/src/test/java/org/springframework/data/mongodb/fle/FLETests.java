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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.bson.BsonBinary;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.types.Binary;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.AliasFor;
import org.springframework.data.convert.PropertyValueConverterFactory;
import org.springframework.data.convert.ValueConverter;
import org.springframework.data.mongodb.config.AbstractMongoClientConfiguration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MongoConversionContext;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions.MongoConverterConfigurationAdapter;
import org.springframework.data.mongodb.core.convert.MongoValueConverter;
import org.springframework.data.mongodb.core.mapping.Encrypted;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.fle.FLETests.Config;
import org.springframework.data.mongodb.util.BsonUtils;
import org.springframework.data.util.Lazy;
import org.springframework.lang.Nullable;
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
import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.client.vault.ClientEncryption;
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

		template.save(person);

		System.out.println("source: " + person);

		Document savedDocument = template.execute(Person.class, collection -> {
			return collection.find(new Document()).first();
		});

		// ssn should look like "ssn": {"$binary": {"base64": "...
		System.out.println("saved: " + savedDocument.toJson());
		assertThat(savedDocument.get("ssn")).isInstanceOf(org.bson.types.Binary.class);
		assertThat(savedDocument.get("wallet")).isInstanceOf(org.bson.types.Binary.class);

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
		EncryptingConverter encryptingConverter(ClientEncryption clientEncryption) {
			return new EncryptingConverter(clientEncryption);
		}

		@Bean
		ClientEncryption clientEncryption(MongoClient mongoClient) {

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
			ClientEncryption clientEncryption = ClientEncryptions.create(clientEncryptionSettings);
			return clientEncryption;
		}
	}

	@Data
	@org.springframework.data.mongodb.core.mapping.Document("test")
	static class Person {

		String id;
		String name;

		@EncryptedField(algorithm = AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic) //
		String ssn;

		@EncryptedField(algorithm = AEAD_AES_256_CBC_HMAC_SHA_512_Random, altKeyName = "mySuperSecretKey") //
		String wallet;
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.FIELD)
	@Encrypted
	@ValueConverter(EncryptingConverter.class)
	@interface EncryptedField {

		@AliasFor(annotation = Encrypted.class, value = "algorithm")
		String algorithm() default "";

		String altKeyName() default "";
	}

	static class EncryptingConverter implements MongoValueConverter<Object, Object> {

		private ClientEncryption clientEncryption;
		private BsonBinary dataKeyId; // should be provided from outside.

		public EncryptingConverter(ClientEncryption clientEncryption) {

			this.clientEncryption = clientEncryption;
			this.dataKeyId = clientEncryption.createDataKey("local",
					new DataKeyOptions().keyAltNames(Collections.singletonList("mySuperSecretKey")));
		}

		@Nullable
		@Override
		public Object read(Object value, MongoConversionContext context) {

			ManualEncryptionContext encryptionContext = buildEncryptionContext(context);
			Object decrypted = encryptionContext.decrypt(value, clientEncryption);
			return decrypted instanceof BsonValue ? BsonUtils.toJavaType((BsonValue) decrypted) : decrypted;
		}

		@Nullable
		@Override
		public BsonBinary write(Object value, MongoConversionContext context) {

			ManualEncryptionContext encryptionContext = buildEncryptionContext(context);
			return encryptionContext.encrypt(value, clientEncryption);
		}

		ManualEncryptionContext buildEncryptionContext(MongoConversionContext context) {
			return new ManualEncryptionContext(context.getProperty(), this.dataKeyId);
		}
	}

	static class ManualEncryptionContext {

		MongoPersistentProperty persistentProperty;
		BsonBinary dataKeyId;
		Lazy<Encrypted> encryption;

		public ManualEncryptionContext(MongoPersistentProperty persistentProperty, BsonBinary dataKeyId) {
			this.persistentProperty = persistentProperty;
			this.dataKeyId = dataKeyId;
			this.encryption = Lazy.of(() -> persistentProperty.findAnnotation(Encrypted.class));
		}

		BsonBinary encrypt(Object value, ClientEncryption clientEncryption) {

			// TODO: check - encryption.get().keyId()

			EncryptOptions encryptOptions = new EncryptOptions(encryption.get().algorithm());

			EncryptedField annotation = persistentProperty.findAnnotation(EncryptedField.class);
			if (annotation != null && !annotation.altKeyName().isBlank()) {
				encryptOptions = encryptOptions.keyAltName(annotation.altKeyName());
			} else {
				encryptOptions = encryptOptions.keyId(this.dataKeyId);
			}

			return clientEncryption.encrypt(BsonUtils.simpleToBsonValue(value), encryptOptions);
		}

		public Object decrypt(Object value, ClientEncryption clientEncryption) {

			if (value instanceof Binary binary) {
				return clientEncryption.decrypt(new BsonBinary(binary.getType(), binary.getData()));
			}
			if (value instanceof BsonBinary binary) {
				return clientEncryption.decrypt(binary);
			}

			// in case the driver has auto decryption (aka .bypassAutoEncryption(true)) active
			// https://github.com/mongodb/mongo-java-driver/blob/master/driver-sync/src/examples/tour/ClientSideEncryptionExplicitEncryptionOnlyTour.java
			return value;
		}
	}
}
