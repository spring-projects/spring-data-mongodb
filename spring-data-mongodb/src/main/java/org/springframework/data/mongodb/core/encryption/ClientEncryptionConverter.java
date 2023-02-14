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
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.util.BsonUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.ObjectUtils;

/**
 * @author Christoph Strobl
 * @since 4.1
 */
public class ClientEncryptionConverter implements EncryptingConverter<Object, Object> {

	private static final Log LOGGER = LogFactory.getLog(ClientEncryptionConverter.class);

	private Encryption<BsonValue, BsonBinary> encryptionProvider;
	private final EncryptionKeyResolver keyProvider;

	public ClientEncryptionConverter(Encryption<BsonValue, BsonBinary> encryptionProvider,
			EncryptionKeyResolver keyProvider) {

		this.encryptionProvider = encryptionProvider;
		this.keyProvider = keyProvider;
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
				LOGGER.debug(String.format("Decrypting %s.%s.", context.getProperty().getOwner().getName(),
						context.getProperty().getName()));
			}

			decryptedValue = encryptionProvider.decrypt((BsonBinary) BsonUtils.simpleToBsonValue(encryptedValue));
			// in case the driver has auto decryption (aka .bypassAutoEncryption(true)) active
			// https://github.com/mongodb/mongo-java-driver/blob/master/driver-sync/src/examples/tour/ClientSideEncryptionExplicitEncryptionOnlyTour.java
			if (encryptedValue == decryptedValue) {
				return decryptedValue;
			}
		}

		MongoPersistentProperty persistentProperty = context.getProperty();

		if (context.getProperty().isCollectionLike() && decryptedValue instanceof Iterable<?> iterable) {
			if (!persistentProperty.isEntity()) {
				Collection<Object> collection = CollectionFactory.createCollection(persistentProperty.getType(), 10);
				iterable.forEach(it -> collection.add(BsonUtils.toJavaType((BsonValue) it)));
				return collection;
			} else {
				Collection<Object> collection = CollectionFactory.createCollection(persistentProperty.getType(), 10);
				iterable.forEach(it -> {
					collection.add(context.getSourceContext().read(BsonUtils.toJavaType((BsonValue) it),
							persistentProperty.getActualType()));
				});
				return collection;
			}
		}

		if (!persistentProperty.isEntity() && decryptedValue instanceof BsonValue bsonValue) {
			return BsonUtils.toJavaType(bsonValue);
		}

		if (persistentProperty.isEntity() && decryptedValue instanceof BsonDocument bsonDocument) {
			return context.getSourceContext().read(BsonUtils.toJavaType(bsonDocument),
					persistentProperty.getTypeInformation());
		}

		return decryptedValue;
	}

	@Override
	public Object encrypt(Object value, EncryptionContext context) {

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(String.format("Encrypting %s.%s.", context.getProperty().getOwner().getName(),
					context.getProperty().getName()));
		}

		MongoPersistentProperty persistentProperty = context.getProperty();
		EncryptionOptions encryptionOptions = new EncryptionOptions(context.lookupEncryptedAnnotation().algorithm());
		encryptionOptions.setKey(keyProvider.getKey(context));

		if (!persistentProperty.isEntity()) {

			if (persistentProperty.isCollectionLike()) {
				return encryptionProvider.encrypt(collectionLikeToBsonValue(value, persistentProperty, context),
						encryptionOptions);
			}
			return encryptionProvider.encrypt(BsonUtils.simpleToBsonValue(value), encryptionOptions);
		}
		if (persistentProperty.isCollectionLike()) {
			return encryptionProvider.encrypt(collectionLikeToBsonValue(value, persistentProperty, context),
					encryptionOptions);
		}

		Object write = context.getSourceContext().write(value);
		if (write instanceof Document doc) {
			return encryptionProvider.encrypt(doc.toBsonDocument(), encryptionOptions);
		}
		return encryptionProvider.encrypt(BsonUtils.simpleToBsonValue(write), encryptionOptions);
	}

	public BsonValue collectionLikeToBsonValue(Object value, MongoPersistentProperty property,
			EncryptionContext context) {

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
						Document write = (Document) context.getSourceContext().write(it, property.getTypeInformation());
						bsonArray.add(write.toBsonDocument());
					});
				} else if (ObjectUtils.isArray(value)) {
					for (Object o : ObjectUtils.toObjectArray(value)) {
						Document write = (Document) context.getSourceContext().write(o, property.getTypeInformation());
						bsonArray.add(write.toBsonDocument());
					}
				}
				return bsonArray;
			}
		}

		return null;
	}

	public EncryptionContext buildEncryptionContext(MongoConversionContext context) {
		return new ExplicitEncryptionContext(context);
	}
}
