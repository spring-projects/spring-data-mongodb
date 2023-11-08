/*
 * Copyright 2013-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.convert;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.data.mongodb.core.mapping.FieldName;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.util.BsonUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.mongodb.DBObject;

/**
 * Wrapper value object for a {@link Document} to be able to access raw values by {@link MongoPersistentProperty}
 * references. The accessors will transparently resolve nested document values that a {@link MongoPersistentProperty}
 * might refer to through a path expression in field names.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 */
class DocumentAccessor {

	private final Bson document;

	/**
	 * Creates a new {@link DocumentAccessor} for the given {@link Document}.
	 *
	 * @param document must be a {@link Document} effectively, must not be {@literal null}.
	 */
	public DocumentAccessor(Bson document) {

		Assert.notNull(document, "Document must not be null");

		if (!(document instanceof Document) && !(document instanceof DBObject)) {
			Assert.isInstanceOf(Document.class, document, "Given Bson must be a Document or DBObject");
		}

		this.document = document;
	}

	/**
	 * @return the underlying {@link Bson document}.
	 * @since 2.1
	 */
	Bson getDocument() {
		return this.document;
	}

	/**
	 * Copies all of the mappings from the given {@link Document} to the underlying target {@link Document}. These
	 * mappings will replace any mappings that the target document had for any of the keys currently in the specified map.
	 *
	 * @param source
	 */
	public void putAll(Document source) {

		Map<String, Object> target = BsonUtils.asMap(document);

		target.putAll(source);
	}

	/**
	 * Puts the given value into the backing {@link Document} based on the coordinates defined through the given
	 * {@link MongoPersistentProperty}. By default this will be the plain field name. But field names might also consist
	 * of path traversals so we might need to create intermediate {@link Document}s.
	 *
	 * @param prop must not be {@literal null}.
	 * @param value can be {@literal null}.
	 */
	public void put(MongoPersistentProperty prop, @Nullable Object value) {

		Assert.notNull(prop, "MongoPersistentProperty must not be null");

		Iterator<String> parts = Arrays.asList(prop.getMongoField().getName().parts()).iterator();
		Bson document = this.document;

		while (parts.hasNext()) {

			String part = parts.next();

			if (parts.hasNext()) {
				document = getOrCreateNestedDocument(part, document);
			} else {
				BsonUtils.addToMap(document, part, value);
			}
		}
	}

	/**
	 * Returns the value the given {@link MongoPersistentProperty} refers to. By default this will be a direct field but
	 * the method will also transparently resolve nested values the {@link MongoPersistentProperty} might refer to through
	 * a path expression in the field name metadata.
	 *
	 * @param property must not be {@literal null}.
	 * @return can be {@literal null}.
	 */
	@Nullable
	public Object get(MongoPersistentProperty property) {
		return BsonUtils.resolveValue(document, getFieldName(property));
	}

	/**
	 * Returns the raw identifier for the given {@link MongoPersistentEntity} or the value of the default identifier
	 * field.
	 *
	 * @param entity must not be {@literal null}.
	 * @return
	 */
	@Nullable
	public Object getRawId(MongoPersistentEntity<?> entity) {
		return entity.hasIdProperty() ? get(entity.getRequiredIdProperty()) : BsonUtils.get(document, FieldName.ID.name());
	}

	/**
	 * Returns whether the underlying {@link Document} has a value ({@literal null} or non-{@literal null}) for the given
	 * {@link MongoPersistentProperty}.
	 *
	 * @param property must not be {@literal null}.
	 * @return {@literal true} if no non {@literal null} value present.
	 */
	@SuppressWarnings("unchecked")
	public boolean hasValue(MongoPersistentProperty property) {

		Assert.notNull(property, "Property must not be null");

		return BsonUtils.hasValue(document, getFieldName(property));
	}

	FieldName getFieldName(MongoPersistentProperty prop) {
		return prop.getMongoField().getName();
	}

	/**
	 * Returns the {@link Document} which either already exists in the given source under the given key, or creates a new
	 * nested one, registers it with the source and returns it.
	 *
	 * @param key must not be {@literal null} or empty.
	 * @param source must not be {@literal null}.
	 * @return
	 */
	private static Document getOrCreateNestedDocument(String key, Bson source) {

		Object existing = BsonUtils.asMap(source).get(key);

		if (existing instanceof Document document) {
			return document;
		}

		Document nested = new Document();
		BsonUtils.addToMap(source, key, nested);

		return nested;
	}
}
