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
package org.springframework.data.mongodb.core.convert.encryption;

import java.util.Collection;
import java.util.LinkedHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.types.Binary;
import org.springframework.core.CollectionFactory;
import org.springframework.data.mongodb.core.convert.MongoConversionContext;
import org.springframework.data.mongodb.core.encryption.Encryption;
import org.springframework.data.mongodb.core.encryption.EncryptionContext;
import org.springframework.data.mongodb.core.encryption.EncryptionKeyResolver;
import org.springframework.data.mongodb.core.encryption.EncryptionOptions;
import org.springframework.data.mongodb.core.mapping.Encrypted;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.util.BsonUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.ObjectUtils;

/**
 * @author Christoph Strobl
 * @since 4.1
 */
public class MongoEncryptionConverter implements EncryptingConverter<Object, Object> {

	private static final Log LOGGER = LogFactory.getLog(MongoEncryptionConverter.class);

	private Encryption<BsonValue, BsonBinary> encryption;
	private final EncryptionKeyResolver keyResolver;

	public MongoEncryptionConverter(Encryption<BsonValue, BsonBinary> encryption, EncryptionKeyResolver keyResolver) {

		this.encryption = encryption;
		this.keyResolver = keyResolver;
	}

	@Nullable
	@Override
	public Object read(Object value, MongoConversionContext context) {

		Object decrypted = EncryptingConverter.super.read(value, context);
		return decrypted instanceof BsonValue ? BsonUtils.toJavaType((BsonValue) decrypted) : decrypted;
	}

	@Override
	public Object decrypt(Object encryptedValue, EncryptionContext context) {

		Object decryptedValue = encryptedValue;
		if (encryptedValue instanceof Binary || encryptedValue instanceof BsonBinary) {

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug(String.format("Decrypting %s.%s.",  getProperty(context).getOwner().getName(),
						getProperty(context).getName()));
			}

			decryptedValue = encryption.decrypt((BsonBinary) BsonUtils.simpleToBsonValue(encryptedValue));

			// in case the driver has auto decryption (aka .bypassAutoEncryption(true)) active
			// https://github.com/mongodb/mongo-java-driver/blob/master/driver-sync/src/examples/tour/ClientSideEncryptionExplicitEncryptionOnlyTour.java
			if (encryptedValue == decryptedValue) {
				return decryptedValue;
			}
		}

		MongoPersistentProperty persistentProperty =  getProperty(context);

		if (getProperty(context).isCollectionLike() && decryptedValue instanceof Iterable<?> iterable) {
			if (!persistentProperty.isEntity()) {
				Collection<Object> collection = CollectionFactory.createCollection(persistentProperty.getType(), 10);
				iterable.forEach(it -> collection.add(BsonUtils.toJavaType((BsonValue) it)));
				return collection;
			} else {
				Collection<Object> collection = CollectionFactory.createCollection(persistentProperty.getType(), 10);
				iterable.forEach(it -> {
					collection.add(context.read(BsonUtils.toJavaType((BsonValue) it),
							persistentProperty.getActualType()));
				});
				return collection;
			}
		}

		if (!persistentProperty.isEntity() && decryptedValue instanceof BsonValue bsonValue) {
			if (persistentProperty.isMap() && persistentProperty.getType() != Document.class) {
				return new LinkedHashMap<>((Document) BsonUtils.toJavaType(bsonValue));

			}
			return BsonUtils.toJavaType(bsonValue);
		}

		if (persistentProperty.isEntity() && decryptedValue instanceof BsonDocument bsonDocument) {
			return context.read(BsonUtils.toJavaType(bsonDocument),
					persistentProperty.getTypeInformation().getType());
		}

		return decryptedValue;
	}

	private MongoPersistentProperty getProperty(EncryptionContext context) {
		return context.getProperty();
	}

	@Override
	public Object encrypt(Object value, EncryptionContext context) {

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(String.format("Encrypting %s.%s.", getProperty(context).getOwner().getName(),
					getProperty(context).getName()));
		}

		MongoPersistentProperty persistentProperty =  getProperty(context);

		Encrypted annotation = persistentProperty.findAnnotation(Encrypted.class);
		if(annotation == null) {
			annotation = persistentProperty.getOwner().findAnnotation(Encrypted.class);
		}

		EncryptionOptions encryptionOptions = new EncryptionOptions(annotation.algorithm());
		encryptionOptions.setKey(keyResolver.getKey(context));

		if (!persistentProperty.isEntity()) {

			if (persistentProperty.isCollectionLike()) {
				return encryption.encrypt(collectionLikeToBsonValue(value, persistentProperty, context), encryptionOptions);
			}
			if (persistentProperty.isMap()) {
				Object convertedMap = context.write(value);
				if (convertedMap instanceof Document document) {
					return encryption.encrypt(document.toBsonDocument(), encryptionOptions);
				}
			}
			return encryption.encrypt(BsonUtils.simpleToBsonValue(value), encryptionOptions);
		}
		if (persistentProperty.isCollectionLike()) {
			return encryption.encrypt(collectionLikeToBsonValue(value, persistentProperty, context), encryptionOptions);
		}

		Object write = context.write(value);
		if (write instanceof Document doc) {
			return encryption.encrypt(doc.toBsonDocument(), encryptionOptions);
		}
		return encryption.encrypt(BsonUtils.simpleToBsonValue(write), encryptionOptions);
	}

	public BsonValue collectionLikeToBsonValue(Object value, MongoPersistentProperty property,
			EncryptionContext context) {

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
					Document write = (Document) context.write(it, property.getTypeInformation());
					bsonArray.add(write.toBsonDocument());
				});
			} else if (ObjectUtils.isArray(value)) {
				for (Object o : ObjectUtils.toObjectArray(value)) {
					Document write = (Document) context.write(o, property.getTypeInformation());
					bsonArray.add(write.toBsonDocument());
				}
			}
			return bsonArray;
		}
	}

	public EncryptionContext buildEncryptionContext(MongoConversionContext context) {
		return new ExplicitEncryptionContext(context);
	}
}
