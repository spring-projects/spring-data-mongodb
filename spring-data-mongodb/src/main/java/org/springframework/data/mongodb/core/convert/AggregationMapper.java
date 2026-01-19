/*
 * Copyright 2025-present the original author or authors.
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

import java.util.Set;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.jspecify.annotations.Nullable;

import org.springframework.data.mapping.MappingException;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.util.BsonUtils;
import org.springframework.util.StringUtils;

/**
 * Aggregation-specific mapper that keeps field references intact and relaxes association path validation.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 4.4
 */
public class AggregationMapper extends QueryMapper {

	public AggregationMapper(MongoConverter converter) {
		super(converter);
	}

	@Override
	@SuppressWarnings("NullAway")
	public Document getMappedObject(Bson query, @Nullable MongoPersistentEntity<?> entity) {

		if (isNestedKeyword(query)) {
			return getMappedKeyword(new Keyword(query), entity);
		}

		Document result = new Document();

		for (String key : BsonUtils.asMap(query).keySet()) {

			// TODO: remove one once QueryMapper can work with Query instances directly
			if (Query.isRestrictedTypeKey(key)) {

				Set<Class<?>> restrictedTypes = BsonUtils.get(query, key);
				this.getConverter().getTypeMapper().writeTypeRestrictions(result, restrictedTypes);
				continue;
			}

			if (isTypeKey(key)) {
				result.put(key, BsonUtils.get(query, key));
				continue;
			}

			if (isKeyword(key)) {
				result.putAll(getMappedKeyword(new Keyword(query, key), entity));
				continue;
			}

			try {

				Field field = createPropertyField(entity, key, getMappingContext());

				// TODO: move to dedicated method
				if (field.getProperty() != null && field.getProperty().isUnwrapped()) {

					Object theNestedObject = BsonUtils.get(query, key);
					Document mappedValue = (Document) getMappedValue(field, theNestedObject);
					if (!StringUtils.hasText(field.getMappedKey())) {
						result.putAll(mappedValue);
					} else {
						result.put(field.getMappedKey(), mappedValue);
					}
				} else {
					result.put(field.getMappedKey(), getMappedValue(field, BsonUtils.get(query, key)));
				}
			} catch (MappingException ex) {
				result.put(key, BsonUtils.get(query, key));
			}
		}

		return result;
	}

	@Override
	protected Document getMappedKeyword(Keyword keyword, @Nullable MongoPersistentEntity<?> entity) {

		if ("$literal".equalsIgnoreCase(keyword.getKey())) {
			return new Document(keyword.getKey(), keyword.getValue());
		}

		return super.getMappedKeyword(keyword, entity);
	}

	@Override
	protected Document getMappedKeyword(Field property, Keyword keyword) {

		if ("$literal".equalsIgnoreCase(keyword.getKey())) {
			return new Document(keyword.getKey(), keyword.getValue());
		}

		return super.getMappedKeyword(property, keyword);
	}

	@Override
	@Nullable
	protected Object getMappedValue(Field documentField, @Nullable Object sourceValue) {

		if (isSystemVariable(sourceValue)) {
			return sourceValue;
		}

		if (isFieldReference(sourceValue)) {
			return mapFieldReference((String) sourceValue, documentField);
		}

		return super.getMappedValue(documentField, sourceValue);
	}

	private boolean isSystemVariable(@Nullable Object sourceValue) {
		return sourceValue instanceof String candidate && candidate.startsWith("$$");
	}

	private boolean isFieldReference(@Nullable Object sourceValue) {

		if (!(sourceValue instanceof String candidate)) {
			return false;
		}

		if (!candidate.startsWith("$") || "$".equals(candidate) || candidate.startsWith("$$")) {
			return false;
		}

		return true;
	}

	private String mapFieldReference(String reference, Field documentField) {

		String path = reference.substring(1);
		if (!StringUtils.hasText(path)) {
			return reference;
		}

		String mappedPath = mapFieldPath(path, documentField.getEntity());
		return mappedPath == null ? reference : "$" + mappedPath;
	}

	@Nullable
	private String mapFieldPath(String path, @Nullable MongoPersistentEntity<?> entity) {

		if (entity == null) {
			return null;
		}

		try {
			return createPropertyField(entity, path, getMappingContext()).getMappedKey();
		} catch (MappingException ex) {
			return null;
		}
	}
}
