/*
 * Copyright 2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import org.springframework.data.mongodb.fle.FLETests.Person;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.security.SecureRandom;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.types.Binary;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.CollectionFactory;
import org.springframework.core.annotation.AliasFor;
import org.springframework.data.convert.PropertyValueConverterFactory;
import org.springframework.data.convert.ValueConverter;
import org.springframework.data.mongodb.config.AbstractReactiveMongoConfiguration;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
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
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import com.mongodb.ClientEncryptionSettings;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.vault.ClientEncryption;
import com.mongodb.reactivestreams.client.vault.ClientEncryptions;

/**
 * @author Christoph Strobl
 * @since 2022/11
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = ReactiveFLETests.Config.class)
public class ReactiveFLETests {

	ClientEncryption encryption;

	@Test
	void xxx() {

		Supplier<String> valueSupplier = new Supplier<String>() {
			@Override
			public String get() {
				System.out.println("invoked");
				return "v1";
			}
		};

		Document source = new Document("name", "value").append("mono", Mono.fromSupplier(() -> "from mono"))
				.append("nested", new Document("n1", Mono.fromSupplier(() -> "from nested mono")));

		resolveValues(Mono.just(source)) //
				.as(StepVerifier::create).consumeNextWith(resolved -> {
					assertThat(resolved).isEqualTo(Document
							.parse("{\"name\": \"value\", \"mono\": \"from mono\", \"nested\" : { \"n1\" : \"from nested mono\"}}"));
				}).verifyComplete();
	}

	private Mono<Document> resolveValues(Mono<Document> document) {
		return document.flatMap(source -> {
			for (Entry<String, Object> entry : source.entrySet()) {
				if (entry.getValue()instanceof Mono<?> valueMono) {
					return valueMono.flatMap(value -> {
						source.put(entry.getKey(), value);
						return resolveValues(Mono.just(source));
					});
				}
				if (entry.getValue()instanceof Document nested) {
					return resolveValues(Mono.just(nested)).map(it -> {
						source.put(entry.getKey(), it);
						return source;
					});
				}
			}
			return Mono.just(source);
		});
	}

	@Autowired ReactiveMongoTemplate template;

	@Test
	void manualEnAndDecryption() {

		Person person = new Person();
		person.id = "id-1";
		person.name = "p1-name";
		person.ssn = "mySecretSSN";

		template.save(person).block();

		System.out.println("source: " + person);

		Flux<Document> result = template.execute(FLETests.Person.class, collection -> {
			return Mono.from(collection.find(new Document()).first());
		});

		System.out.println("encrypted: " + result.blockFirst().toJson());

		Person id = template.query(Person.class).matching(where("id").is(person.id)).first().block();
		System.out.println("decrypted: " + id);
	}

	@Data
	@org.springframework.data.mongodb.core.mapping.Document("test")
	static class Person {

		String id;
		String name;

		@EncryptedField(algorithm = AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic) //
		String ssn;
	}

	@Configuration
	static class Config extends AbstractReactiveMongoConfiguration {

		@Autowired ApplicationContext applicationContext;

		@Override
		protected String getDatabaseName() {
			return "fle-test";
		}

		@Override
		protected void configureConverters(MongoConverterConfigurationAdapter converterConfigurationAdapter) {

			converterConfigurationAdapter
					.registerPropertyValueConverterFactory(PropertyValueConverterFactory.beanFactoryAware(applicationContext));
		}

		@Bean
		@Override
		public MongoClient reactiveMongoClient() {
			return super.reactiveMongoClient();
		}

		@Bean
		ReactiveEncryptingConverter encryptingConverter(ClientEncryption clientEncryption) {
			return new ReactiveEncryptingConverter(clientEncryption);
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
			Mono.from(keyVaultCollection.drop()).block();
			// Ensure that two data keys cannot share the same keyAltName.
			keyVaultCollection.createIndex(Indexes.ascending("keyAltNames"),
					new IndexOptions().unique(true).partialFilterExpression(Filters.exists("keyAltNames")));

			MongoCollection<Document> collection = mongoClient.getDatabase(getDatabaseName()).getCollection("test");
			Mono.from(collection.drop()).block(); // Clear old data

			// Create the ClientEncryption instance
			ClientEncryptionSettings clientEncryptionSettings = ClientEncryptionSettings.builder()
					.keyVaultMongoClientSettings(
							MongoClientSettings.builder().applyConnectionString(new ConnectionString("mongodb://localhost")).build())
					.keyVaultNamespace(keyVaultNamespace.getFullName()).kmsProviders(kmsProviders).build();
			ClientEncryption clientEncryption = ClientEncryptions.create(clientEncryptionSettings);
			return clientEncryption;
		}
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.FIELD)
	@Encrypted
	@ValueConverter(ReactiveEncryptingConverter.class)
	@interface EncryptedField {

		@AliasFor(annotation = Encrypted.class, value = "algorithm")
		String algorithm() default "";

		String altKeyName() default "";
	}

	static class ReactiveEncryptingConverter implements MongoValueConverter<Object, Object> {

		private ClientEncryption clientEncryption;
		private BsonBinary dataKeyId; // should be provided from outside.

		public ReactiveEncryptingConverter(ClientEncryption clientEncryption) {

			this.clientEncryption = clientEncryption;
			this.dataKeyId = Mono.from(clientEncryption.createDataKey("local",
					new DataKeyOptions().keyAltNames(Collections.singletonList("mySuperSecretKey")))).block();
		}

		@Nullable
		@Override
		public Object read(Object value, MongoConversionContext context) {

			ManualEncryptionContext encryptionContext = buildEncryptionContext(context);
			Object decrypted = null;
			try {
				decrypted = encryptionContext.decrypt(value, clientEncryption);
			} catch (ExecutionException e) {
				throw new RuntimeException(e);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			return decrypted instanceof BsonValue ? BsonUtils.toJavaType((BsonValue) decrypted) : decrypted;
		}

		@Nullable
		@Override
		public Publisher<BsonBinary> write(Object value, MongoConversionContext context) {

			ManualEncryptionContext encryptionContext = buildEncryptionContext(context);
			return encryptionContext.encrypt(value, clientEncryption);
		}

		ManualEncryptionContext buildEncryptionContext(MongoConversionContext context) {
			return new ManualEncryptionContext(context, this.dataKeyId);
		}
	}

	static class ManualEncryptionContext {

		MongoConversionContext context;
		MongoPersistentProperty persistentProperty;
		BsonBinary dataKeyId;
		Lazy<Encrypted> encryption;

		public ManualEncryptionContext(MongoConversionContext context, BsonBinary dataKeyId) {
			this.context = context;
			this.persistentProperty = context.getProperty();
			this.dataKeyId = dataKeyId;
			this.encryption = Lazy.of(() -> persistentProperty.findAnnotation(Encrypted.class));
		}

		Publisher<BsonBinary> encrypt(Object value, ClientEncryption clientEncryption) {

			// TODO: check - encryption.get().keyId()

			EncryptOptions encryptOptions = new EncryptOptions(encryption.get().algorithm());

			EncryptedField annotation = persistentProperty.findAnnotation(EncryptedField.class);
			if (annotation != null && !annotation.altKeyName().isBlank()) {
				if (annotation.altKeyName().startsWith("/")) {
					String fieldName = annotation.altKeyName().replace("/", "");
					Object altKeyNameValue = context.getValue(fieldName);
					encryptOptions = encryptOptions.keyAltName(altKeyNameValue.toString());
				} else {
					encryptOptions = encryptOptions.keyAltName(annotation.altKeyName());
				}
			} else {
				encryptOptions = encryptOptions.keyId(this.dataKeyId);
			}

			System.out.println(
					"encrypting with: " + (StringUtils.hasText(encryptOptions.getKeyAltName()) ? encryptOptions.getKeyAltName()
							: encryptOptions.getKeyId()));

			if (!persistentProperty.isEntity()) {

				if (persistentProperty.isCollectionLike()) {
					return clientEncryption.encrypt(collectionLikeToBsonValue(value), encryptOptions);
				}
				return clientEncryption.encrypt(BsonUtils.simpleToBsonValue(value), encryptOptions);
			}
			if (persistentProperty.isCollectionLike()) {
				return clientEncryption.encrypt(collectionLikeToBsonValue(value), encryptOptions);
			}

			Object write = context.write(value);
			if (write instanceof Document doc) {
				return clientEncryption.encrypt(doc.toBsonDocument(), encryptOptions);
			}
			return clientEncryption.encrypt(BsonUtils.simpleToBsonValue(write), encryptOptions);
		}

		public BsonValue collectionLikeToBsonValue(Object value) {

			if (persistentProperty.isCollectionLike()) {

				BsonArray bsonArray = new BsonArray();
				if (!persistentProperty.isEntity()) {
					if (value instanceof Collection values) {
						values.forEach(it -> bsonArray.add(BsonUtils.simpleToBsonValue(it)));
					} else if (ObjectUtils.isArray(value)) {
						for (Object o : ObjectUtils.toObjectArray(value)) {
							bsonArray.add(BsonUtils.simpleToBsonValue(o));
						}
					}
					return bsonArray;
				} else {
					if (value instanceof Collection values) {
						values.forEach(it -> {
							Document write = (Document) context.write(it, persistentProperty.getTypeInformation());
							bsonArray.add(write.toBsonDocument());
						});
					} else if (ObjectUtils.isArray(value)) {
						for (Object o : ObjectUtils.toObjectArray(value)) {
							Document write = (Document) context.write(o, persistentProperty.getTypeInformation());
							bsonArray.add(write.toBsonDocument());
						}
					}
					return bsonArray;
				}
			}

			if (!persistentProperty.isEntity()) {
				if (persistentProperty.isCollectionLike()) {

					if (persistentProperty.isEntity()) {

					}
				}
			}

			return null;
		}

		public Object decrypt(Object value, ClientEncryption clientEncryption) throws ExecutionException, InterruptedException {

			// this was a hack to avoid the 60 sec timeout of the key cache
			// ClientEncryptionSettings settings = (ClientEncryptionSettings) new DirectFieldAccessor(clientEncryption)
			// .getPropertyValue("options");
			// clientEncryption = ClientEncryptions.create(settings);

			Object r = value;
			if (value instanceof Binary binary) {
				r = clientEncryption.decrypt(new BsonBinary(binary.getType(), binary.getData()));
			}
			if (value instanceof BsonBinary binary) {
				r = clientEncryption.decrypt(binary);
			}

			// in case the driver has auto decryption (aka .bypassAutoEncryption(true)) active
			// https://github.com/mongodb/mongo-java-driver/blob/master/driver-sync/src/examples/tour/ClientSideEncryptionExplicitEncryptionOnlyTour.java
			if (value == r) {
				return r;
			}

			if(r instanceof Mono mono) {
				return mono.map(result -> {
					if (persistentProperty.isCollectionLike() && result instanceof Iterable<?> iterable) {
						if (!persistentProperty.isEntity()) {
							Collection<Object> collection = CollectionFactory.createCollection(persistentProperty.getType(), 10);
							iterable.forEach(it -> collection.add(BsonUtils.toJavaType((BsonValue) it)));
							return collection;
						} else {
							Collection<Object> collection = CollectionFactory.createCollection(persistentProperty.getType(), 10);
							iterable.forEach(it -> {
								collection.add(context.read(BsonUtils.toJavaType((BsonValue) it), persistentProperty.getActualType()));
							});
							return collection;
						}
					}

					if (!persistentProperty.isEntity() && result instanceof BsonValue bsonValue) {
						return BsonUtils.toJavaType(bsonValue);
					}

					if (persistentProperty.isEntity() && result instanceof BsonDocument bsonDocument) {
						return context.read(BsonUtils.toJavaType(bsonDocument), persistentProperty.getTypeInformation());
					}
					return result;
				}).toFuture().get();
			}

			return r;
		}
	}

}
