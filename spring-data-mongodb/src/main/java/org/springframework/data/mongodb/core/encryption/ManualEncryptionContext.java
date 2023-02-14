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

import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.client.vault.ClientEncryption;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.types.Binary;
import org.springframework.core.CollectionFactory;
import org.springframework.data.mongodb.core.convert.MongoConversionContext;
import org.springframework.data.mongodb.core.mapping.Encrypted;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.util.BsonUtils;
import org.springframework.data.util.Lazy;
import org.springframework.lang.Nullable;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * @author Christoph Strobl
 * @since 4.1
 */
public class ManualEncryptionContext implements EncryptionContext {

	private final MongoConversionContext conversionContext;
	private final MongoPersistentProperty persistentProperty;
	private final KeyIdProvider<BsonBinary> keyIdProvider;
	private final Lazy<Encrypted> encryption;

	public ManualEncryptionContext(MongoConversionContext conversionContext, KeyIdProvider<BsonBinary> keyIdProvider) {
		this.conversionContext = conversionContext;
		this.persistentProperty = conversionContext.getProperty();
		this.encryption = Lazy.of(() -> persistentProperty.findAnnotation(Encrypted.class));
		this.keyIdProvider = keyIdProvider;
	}

	BsonBinary encrypt(Object value, ClientEncryption clientEncryption) {

		// TODO: check - encryption.get().keyId()

		EncryptOptions encryptOptions = new EncryptOptions(encryption.get().algorithm());

		ExplicitlyEncrypted annotation = persistentProperty.findAnnotation(ExplicitlyEncrypted.class);
		if (annotation != null && !annotation.altKeyName().isBlank()) {
			if (annotation.altKeyName().startsWith("/")) {
				String fieldName = annotation.altKeyName().replace("/", "");
				Object altKeyNameValue = conversionContext.getValue(fieldName);
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
				return clientEncryption.encrypt(collectionLikeToBsonValue(value), encryptOptions);
			}
			return clientEncryption.encrypt(BsonUtils.simpleToBsonValue(value), encryptOptions);
		}
		if (persistentProperty.isCollectionLike()) {
			return clientEncryption.encrypt(collectionLikeToBsonValue(value), encryptOptions);
		}

		Object write = conversionContext.write(value);
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
						Document write = (Document) conversionContext.write(it, persistentProperty.getTypeInformation());
						bsonArray.add(write.toBsonDocument());
					});
				} else if (ObjectUtils.isArray(value)) {
					for (Object o : ObjectUtils.toObjectArray(value)) {
						Document write = (Document) conversionContext.write(0, persistentProperty.getTypeInformation());
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

	public Object decrypt(Object value, ClientEncryption clientEncryption) {

		// this was a hack to avoid the 60 sec timeout of the key cache
		// ClientEncryptionSettings settings = (ClientEncryptionSettings) new DirectFieldAccessor(clientEncryption)
		// .getPropertyValue("options");
		// clientEncryption = ClientEncryptions.create(settings);

		Object result = value;
		if (value instanceof Binary binary) {
			result = clientEncryption.decrypt(new BsonBinary(binary.getType(), binary.getData()));
		}
		if (value instanceof BsonBinary binary) {
			result = clientEncryption.decrypt(binary);
		}

		// in case the driver has auto decryption (aka .bypassAutoEncryption(true)) active
		// https://github.com/mongodb/mongo-java-driver/blob/master/driver-sync/src/examples/tour/ClientSideEncryptionExplicitEncryptionOnlyTour.java
		if (value == result) {
			return result;
		}

		if (persistentProperty.isCollectionLike() && result instanceof Iterable<?> iterable) {
			if (!persistentProperty.isEntity()) {
				Collection<Object> collection = CollectionFactory.createCollection(persistentProperty.getType(), 10);
				iterable.forEach(it -> collection.add(BsonUtils.toJavaType((BsonValue) it)));
				return collection;
			} else {
				Collection<Object> collection = CollectionFactory.createCollection(persistentProperty.getType(), 10);
				iterable.forEach(it -> {
					collection.add(conversionContext.read(BsonUtils.toJavaType((BsonValue) it), persistentProperty.getActualType()));
				});
				return collection;
			}
		}

		if (!persistentProperty.isEntity() && result instanceof BsonValue bsonValue) {
			return BsonUtils.toJavaType(bsonValue);
		}

		if (persistentProperty.isEntity() && result instanceof BsonDocument bsonDocument) {
			return conversionContext.read(BsonUtils.toJavaType(bsonDocument), persistentProperty.getTypeInformation());
		}

		return result;
	}

	@Override
	public MongoPersistentProperty getProperty() {
		return conversionContext.getProperty();
	}

	@Nullable
	@Override
	public Object lookupValue(String path) {
		return conversionContext.getValue(path);
	}

	@Override
	public MongoConversionContext getConversionContext() {
		return conversionContext;
	}
}
