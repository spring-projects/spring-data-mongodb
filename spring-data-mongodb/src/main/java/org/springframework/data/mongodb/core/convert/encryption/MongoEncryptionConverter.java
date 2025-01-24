/*
 * Copyright 2023-2025 the original author or authors.
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

import static java.util.Arrays.*;
import static java.util.Collections.*;
import static org.springframework.data.mongodb.core.EncryptionAlgorithms.*;
import static org.springframework.data.mongodb.core.encryption.EncryptionOptions.*;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
import org.springframework.data.mongodb.core.encryption.EncryptionKey;
import org.springframework.data.mongodb.core.encryption.EncryptionKeyResolver;
import org.springframework.data.mongodb.core.encryption.EncryptionOptions;
import org.springframework.data.mongodb.core.mapping.Encrypted;
import org.springframework.data.mongodb.core.mapping.ExplicitEncrypted;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.util.BsonUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.ObjectUtils;

/**
 * Default implementation of {@link EncryptingConverter}. Properties used with this converter must be annotated with
 * {@link Encrypted @Encrypted} to provide key and algorithm metadata.
 *
 * @author Christoph Strobl
 * @author Ross Lawley
 * @since 4.1
 */
public class MongoEncryptionConverter implements EncryptingConverter<Object, Object> {

	private static final Log LOGGER = LogFactory.getLog(MongoEncryptionConverter.class);
	private static final String EQUALITY_OPERATOR = "$eq";
	private static final List<String> RANGE_OPERATORS = asList("$gt", "$gte", "$lt", "$lte");

	private final Encryption<BsonValue, BsonBinary> encryption;
	private final EncryptionKeyResolver keyResolver;

	public MongoEncryptionConverter(Encryption<BsonValue, BsonBinary> encryption, EncryptionKeyResolver keyResolver) {

		this.encryption = encryption;
		this.keyResolver = keyResolver;
	}

	@Nullable
	@Override
	public Object read(Object value, MongoConversionContext context) {

		Object decrypted = EncryptingConverter.super.read(value, context);
		return decrypted instanceof BsonValue bsonValue ? BsonUtils.toJavaType(bsonValue) : decrypted;
	}

	@Override
	public Object decrypt(Object encryptedValue, EncryptionContext context) {

		Object decryptedValue = encryptedValue;
		if (encryptedValue instanceof Binary || encryptedValue instanceof BsonBinary) {

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug(String.format("Decrypting %s.%s.", getProperty(context).getOwner().getName(),
						getProperty(context).getName()));
			}

			decryptedValue = encryption.decrypt((BsonBinary) BsonUtils.simpleToBsonValue(encryptedValue));

			// in case the driver has auto decryption (aka .bypassAutoEncryption(true)) active
			// https://github.com/mongodb/mongo-java-driver/blob/master/driver-sync/src/examples/tour/ClientSideEncryptionExplicitEncryptionOnlyTour.java
			if (encryptedValue == decryptedValue) {
				return decryptedValue;
			}
		}

		MongoPersistentProperty persistentProperty = getProperty(context);
		if (getProperty(context).isCollectionLike() && decryptedValue instanceof Iterable<?> iterable) {

			int size = iterable instanceof Collection<?> c ? c.size() : 10;

			if (!persistentProperty.isEntity()) {
				Collection<Object> collection = CollectionFactory.createCollection(persistentProperty.getType(), size);
				iterable.forEach(it -> {
					if (it instanceof BsonValue bsonValue) {
						collection.add(BsonUtils.toJavaType(bsonValue));
					} else {
						collection.add(context.read(it, persistentProperty.getActualType()));
					}
				});

				return collection;
			} else {
				Collection<Object> collection = CollectionFactory.createCollection(persistentProperty.getType(), size);
				iterable.forEach(it -> {
					if (it instanceof BsonValue bsonValue) {
						collection.add(context.read(BsonUtils.toJavaType(bsonValue), persistentProperty.getActualType()));
					} else {
						collection.add(context.read(it, persistentProperty.getActualType()));
					}
				});
				return collection;
			}
		}

		if (!persistentProperty.isEntity() && persistentProperty.isMap()) {
			if (persistentProperty.getType() != Document.class) {
				if (decryptedValue instanceof BsonValue bsonValue) {
					return new LinkedHashMap<>((Document) BsonUtils.toJavaType(bsonValue));
				}
				if (decryptedValue instanceof Document document) {
					return new LinkedHashMap<>(document);
				}
				if (decryptedValue instanceof Map map) {
					return map;
				}
			}
		}

		if (persistentProperty.isEntity() && decryptedValue instanceof BsonDocument bsonDocument) {
			return context.read(BsonUtils.toJavaType(bsonDocument), persistentProperty.getTypeInformation().getType());
		}

		if (persistentProperty.isEntity() && decryptedValue instanceof Document document) {
			return context.read(document, persistentProperty.getTypeInformation().getType());
		}

		return decryptedValue;
	}

	@Override
	public Object encrypt(Object value, EncryptionContext context) {

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(String.format("Encrypting %s.%s.", getProperty(context).getOwner().getName(),
					getProperty(context).getName()));
		}

		MongoPersistentProperty persistentProperty = getProperty(context);

		Encrypted annotation = persistentProperty.findAnnotation(Encrypted.class);
		if (annotation == null) {
			annotation = persistentProperty.getOwner().findAnnotation(Encrypted.class);
		}

		if (annotation == null) {
			throw new IllegalStateException(String.format("Property %s.%s is not annotated with @Encrypted",
					getProperty(context).getOwner().getName(), getProperty(context).getName()));
		}

		boolean encryptExpression = false;
		String algorithm = annotation.algorithm();
		EncryptionKey key = keyResolver.getKey(context);
		EncryptionOptions encryptionOptions = new EncryptionOptions(algorithm, key);
		String fieldNameAndQueryOperator = context.getFieldNameAndQueryOperator();

		ExplicitEncrypted explicitEncryptedAnnotation = persistentProperty.findAnnotation(ExplicitEncrypted.class);
		if (explicitEncryptedAnnotation != null) {
			QueryableEncryptionOptions queryableEncryptionOptions = QueryableEncryptionOptions.none();
			String rangeOptions = explicitEncryptedAnnotation.rangeOptions();
			if (!rangeOptions.isEmpty()) {
				queryableEncryptionOptions = queryableEncryptionOptions.rangeOptions(Document.parse(rangeOptions));
			}

			if (explicitEncryptedAnnotation.contentionFactor() >= 0) {
				queryableEncryptionOptions = queryableEncryptionOptions
						.contentionFactor(explicitEncryptedAnnotation.contentionFactor());
			}

			boolean isPartOfARangeQuery = algorithm.equalsIgnoreCase(RANGE) && fieldNameAndQueryOperator != null;
			if (isPartOfARangeQuery) {
				encryptExpression = true;
				queryableEncryptionOptions = queryableEncryptionOptions.queryType("range");
			}
			encryptionOptions = new EncryptionOptions(algorithm, key, queryableEncryptionOptions);
		}

		if (encryptExpression) {
			return encryptExpression(fieldNameAndQueryOperator, value, encryptionOptions);
		} else {
			return encryptValue(value, context, persistentProperty, encryptionOptions);
		}
	}

	private BsonBinary encryptValue(Object value, EncryptionContext context, MongoPersistentProperty persistentProperty,
			EncryptionOptions encryptionOptions) {
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

	/**
	 * Encrypts a range query expression.
	 *
	 * <p>The mongodb-crypt {@code encryptExpression} has strict formatting requirements so this method
	 * ensures these requirements are met and then picks out and returns just the value for use with a range query.
	 *
	 * @param fieldNameAndQueryOperator field name and query operator
	 * @param value the value of the expression to be encrypted
	 * @param encryptionOptions the options
	 * @return the encrypted range value for use in a range query
	 */
	private BsonValue encryptExpression(String fieldNameAndQueryOperator, Object value,
			EncryptionOptions encryptionOptions) {
		BsonValue doc = BsonUtils.simpleToBsonValue(value);

		String fieldName = fieldNameAndQueryOperator;
		String queryOperator = EQUALITY_OPERATOR;

		int pos = fieldNameAndQueryOperator.lastIndexOf(".$");
		if (pos > -1) {
			fieldName = fieldNameAndQueryOperator.substring(0, pos);
			queryOperator = fieldNameAndQueryOperator.substring(pos + 1);
		}

		if (!RANGE_OPERATORS.contains(queryOperator)) {
			throw new AssertionError(String.format("Not a valid range query. Querying a range encrypted field but the "
					+ "query operator '%s' for field path '%s' is not a range query.", queryOperator, fieldName));
		}

		BsonDocument encryptExpression = new BsonDocument("$and",
				new BsonArray(singletonList(new BsonDocument(fieldName, new BsonDocument(queryOperator, doc)))));

		BsonDocument result = encryption.encryptExpression(encryptExpression, encryptionOptions);
		return result.getArray("$and").get(0).asDocument().getDocument(fieldName).getBinary(queryOperator);
	}

	private BsonValue collectionLikeToBsonValue(Object value, MongoPersistentProperty property,
			EncryptionContext context) {

		BsonArray bsonArray = new BsonArray();
		boolean isEntity = property.isEntity();

		if (value instanceof Collection<?> values) {
			values.forEach(it -> {

				if (isEntity) {
					Document document = (Document) context.write(it, property.getTypeInformation());
					bsonArray.add(document == null ? null : document.toBsonDocument());
				} else {
					bsonArray.add(BsonUtils.simpleToBsonValue(it));
				}
			});
		} else if (ObjectUtils.isArray(value)) {

			for (Object o : ObjectUtils.toObjectArray(value)) {

				if (isEntity) {
					Document document = (Document) context.write(o, property.getTypeInformation());
					bsonArray.add(document == null ? null : document.toBsonDocument());
				} else {
					bsonArray.add(BsonUtils.simpleToBsonValue(o));
				}
			}
		}

		return bsonArray;
	}

	@Override
	public EncryptionContext buildEncryptionContext(MongoConversionContext context) {
		return new ExplicitEncryptionContext(context);
	}

	protected MongoPersistentProperty getProperty(EncryptionContext context) {
		return context.getProperty();
	}
}
