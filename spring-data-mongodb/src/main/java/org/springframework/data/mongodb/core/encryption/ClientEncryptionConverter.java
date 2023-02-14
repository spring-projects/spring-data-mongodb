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

import java.util.Collection;
import java.util.Collections;
import java.util.function.Supplier;

import com.mongodb.client.model.vault.EncryptOptions;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.types.Binary;
import org.springframework.core.CollectionFactory;
import org.springframework.data.mongodb.core.convert.MongoConversionContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.util.BsonUtils;
import org.springframework.data.util.Lazy;
import org.springframework.lang.Nullable;

import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.vault.ClientEncryption;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * @author Christoph Strobl
 */
public class ClientEncryptionConverter implements EncryptingConverter<Object, Object> {

	private Supplier<ClientEncryption> clientEncryption;
	private final KeyIdProvider<BsonBinary> keyIdProvider;

	public ClientEncryptionConverter(ClientEncryption clientEncryption, KeyIdProvider<BsonBinary> keyIdProvider) {
		this(() -> clientEncryption, keyIdProvider);
	}

	public ClientEncryptionConverter(Supplier<ClientEncryption> clientEncryption, KeyIdProvider<BsonBinary> keyIdProvider) {

		this.clientEncryption = clientEncryption;
		if (keyIdProvider != null) {
			this.keyIdProvider = keyIdProvider;
		} else {
			Lazy<BsonBinary> dataKey = Lazy.of(() -> clientEncryption.get().createDataKey("local",
					new DataKeyOptions().keyAltNames(Collections.singletonList("mySuperSecretKey"))));
			this.keyIdProvider = (ctx) -> dataKey.get();
		}
	}

	@Nullable
	@Override
	public Object read(Object value, MongoConversionContext context) {

		Object decrypted = EncryptingConverter.super.read(value, context);
		return decrypted instanceof BsonValue ? BsonUtils.toJavaType((BsonValue) decrypted) : decrypted;
	}

	@Override
	public Object decrypt(Object value, EncryptionContext context) {

		Object result = value;
		if (value instanceof Binary binary) {
			result = clientEncryption.get().decrypt(new BsonBinary(binary.getType(), binary.getData()));
		}
		if (value instanceof BsonBinary binary) {
			result = clientEncryption.get().decrypt(binary);
		}

		// in case the driver has auto decryption (aka .bypassAutoEncryption(true)) active
		// https://github.com/mongodb/mongo-java-driver/blob/master/driver-sync/src/examples/tour/ClientSideEncryptionExplicitEncryptionOnlyTour.java
		if (value == result) {
			return result;
		}

		MongoPersistentProperty persistentProperty = context.getProperty();
		if (context.getProperty().isCollectionLike() && result instanceof Iterable<?> iterable) {
			if (!persistentProperty.isEntity()) {
				Collection<Object> collection = CollectionFactory.createCollection(persistentProperty.getType(), 10);
				iterable.forEach(it -> collection.add(BsonUtils.toJavaType((BsonValue) it)));
				return collection;
			} else {
				Collection<Object> collection = CollectionFactory.createCollection(persistentProperty.getType(), 10);
				iterable.forEach(it -> {
					collection.add(context.getConversionContext().read(BsonUtils.toJavaType((BsonValue) it), persistentProperty.getActualType()));
				});
				return collection;
			}
		}

		if (!persistentProperty.isEntity() && result instanceof BsonValue bsonValue) {
			return BsonUtils.toJavaType(bsonValue);
		}

		if (persistentProperty.isEntity() && result instanceof BsonDocument bsonDocument) {
			return context.getConversionContext().read(BsonUtils.toJavaType(bsonDocument), persistentProperty.getTypeInformation());
		}

		return result;
	}

	@Override
	public Object encrypt(Object value, EncryptionContext context) {

		MongoPersistentProperty persistentProperty = context.getProperty();
		EncryptOptions encryptOptions = new EncryptOptions(context.getAlgorithm());

		ExplicitlyEncrypted annotation = persistentProperty.findAnnotation(ExplicitlyEncrypted.class);
		if (annotation != null && !annotation.altKeyName().isBlank()) {
			if (annotation.altKeyName().startsWith("/")) {
				String fieldName = annotation.altKeyName().replace("/", "");
				Object altKeyNameValue = context.lookupValue(fieldName);
				encryptOptions = encryptOptions.keyAltName(altKeyNameValue.toString());
			} else {
				encryptOptions = encryptOptions.keyAltName(annotation.altKeyName());
			}
		} else {
			encryptOptions = encryptOptions.keyId(keyIdProvider.getKeyId(persistentProperty));
		}

		System.out.println(
				"encrypting with: " + (StringUtils.hasText(encryptOptions.getKeyAltName()) ? encryptOptions.getKeyAltName()
						: encryptOptions.getKeyId()));

		if (!persistentProperty.isEntity()) {

			if (persistentProperty.isCollectionLike()) {
				return clientEncryption.get().encrypt(collectionLikeToBsonValue(value, persistentProperty, context), encryptOptions);
			}
			return clientEncryption.get().encrypt(BsonUtils.simpleToBsonValue(value), encryptOptions);
		}
		if (persistentProperty.isCollectionLike()) {
			return clientEncryption.get().encrypt(collectionLikeToBsonValue(value, persistentProperty, context), encryptOptions);
		}

		Object write = context.getConversionContext().write(value);
		if (write instanceof Document doc) {
			return clientEncryption.get().encrypt(doc.toBsonDocument(), encryptOptions);
		}
		return clientEncryption.get().encrypt(BsonUtils.simpleToBsonValue(write), encryptOptions);
	}

	public BsonValue collectionLikeToBsonValue(Object value, MongoPersistentProperty property, EncryptionContext context) {

		if (property.isCollectionLike()) {

			BsonArray bsonArray = new BsonArray();
			if (!property.isEntity()) {
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
						Document write = (Document) context.getConversionContext().write(it, property.getTypeInformation());
						bsonArray.add(write.toBsonDocument());
					});
				} else if (ObjectUtils.isArray(value)) {
					for (Object o : ObjectUtils.toObjectArray(value)) {
						Document write = (Document) context.getConversionContext().write(o, property.getTypeInformation());
						bsonArray.add(write.toBsonDocument());
					}
				}
				return bsonArray;
			}
		}

		return null;
	}

	public ManualEncryptionContext buildEncryptionContext(MongoConversionContext context) {
		return new ManualEncryptionContext(context, this.keyIdProvider);
	}
}
