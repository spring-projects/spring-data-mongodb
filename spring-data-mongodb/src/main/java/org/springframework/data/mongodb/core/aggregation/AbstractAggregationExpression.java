/*
 * Copyright 2016-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.aggregation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;

import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Order;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.FieldReference;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Support class for {@link AggregationExpression} implementations.
 *
 * @author Christoph Strobl
 * @author Matt Morrissette
 * @author Mark Paluch
 * @since 1.10
 */
abstract class AbstractAggregationExpression implements AggregationExpression {

	private final Object value;

	protected AbstractAggregationExpression(Object value) {
		this.value = value;
	}

	@Override
	public Document toDocument(AggregationOperationContext context) {
		return toDocument(this.value, context);
	}

	public Document toDocument(Object value, AggregationOperationContext context) {
		return new Document(getMongoMethod(), unpack(value, context));
	}

	protected static List<Field> asFields(String... fieldRefs) {

		if (ObjectUtils.isEmpty(fieldRefs)) {
			return Collections.emptyList();
		}

		return Fields.fields(fieldRefs).asList();
	}

	@SuppressWarnings("unchecked")
	private Object unpack(Object value, AggregationOperationContext context) {

		if (value instanceof AggregationExpression aggregationExpression) {
			return aggregationExpression.toDocument(context);
		}

		if (value instanceof Field field) {
			return context.getReference(field).toString();
		}

		if (value instanceof Fields fields) {

			List<Object> mapped = new ArrayList<>(fields.size());

			for (Field field : fields) {
				mapped.add(unpack(field, context));
			}

			return mapped;
		}

		if (value instanceof Sort sort) {

			Document sortDoc = new Document();
			for (Order order : sort) {

				// Check reference
				FieldReference reference = context.getReference(order.getProperty());
				sortDoc.put(reference.getRaw(), order.isAscending() ? 1 : -1);
			}
			return sortDoc;
		}

		if (value instanceof List) {

			List<Object> sourceList = (List<Object>) value;
			List<Object> mappedList = new ArrayList<>(sourceList.size());

			for (Object o : sourceList) {
				mappedList.add(unpack(o, context));
			}

			return mappedList;
		}

		if (value instanceof Map) {

			Document targetDocument = new Document();

			Map<String, Object> sourceMap = (Map<String, Object>) value;
			sourceMap.forEach((k, v) -> targetDocument.append(k, unpack(v, context)));

			return targetDocument;
		}

		if (value instanceof SystemVariable) {
			return value.toString();
		}

		return value;
	}

	@SuppressWarnings("unchecked")
	protected List<Object> append(Object value, Expand expandList) {

		if (this.value instanceof List) {

			List<Object> clone = new ArrayList<>((List<Object>) this.value);

			if (value instanceof Collection<?> collection && Expand.EXPAND_VALUES.equals(expandList)) {
				clone.addAll(collection);
			} else {
				clone.add(value);
			}

			return clone;
		}

		return Arrays.asList(this.value, value);
	}

	/**
	 * Expand a nested list of values to single entries or keep the list.
	 */
	protected enum Expand {
		EXPAND_VALUES, KEEP_SOURCE
	}

	protected List<Object> append(Object value) {
		return append(value, Expand.EXPAND_VALUES);
	}

	@SuppressWarnings({ "unchecked" })
	protected Map<String, Object> append(String key, Object value) {

		Assert.isInstanceOf(Map.class, this.value, "Value must be a type of Map");

		return append((Map<String, Object>) this.value, key, value);
	}

	private Map<String, Object> append(Map<String, Object> existing, String key, Object value) {

		Map<String, Object> clone = new LinkedHashMap<>(existing);
		clone.put(key, value);
		return clone;
	}

	@SuppressWarnings("rawtypes")
	protected Map<String, Object> appendTo(String key, Object value) {

		Assert.isInstanceOf(Map.class, this.value, "Value must be a type of Map");

		if (this.value instanceof Map map) {

			Map<String, Object> target = new HashMap<>(map);
			if (!target.containsKey(key)) {
				target.put(key, value);
				return target;
			}
			target.computeIfPresent(key, (k, v) -> {

				if (v instanceof List<?> list) {
					List<Object> targetList = new ArrayList<>(list);
					targetList.add(value);
					return targetList;
				}
				return Arrays.asList(v, value);
			});
			return target;
		}
		throw new IllegalStateException(
				String.format("Cannot append value to %s type", ObjectUtils.nullSafeClassName(this.value)));

	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected Map<String, Object> remove(String key) {

		Assert.isInstanceOf(Map.class, this.value, "Value must be a type of Map");

		Map<String, Object> clone = new LinkedHashMap<>((java.util.Map) this.value);
		clone.remove(key);
		return clone;
	}

	/**
	 * Append the given key at the position in the underlying {@link LinkedHashMap}.
	 *
	 * @param index
	 * @param key
	 * @param value
	 * @return
	 * @since 3.1
	 */
	@SuppressWarnings({ "unchecked" })
	protected Map<String, Object> appendAt(int index, String key, Object value) {

		Assert.isInstanceOf(Map.class, this.value, "Value must be a type of Map");

		Map<String, Object> clone = new LinkedHashMap<>();

		int i = 0;
		for (Map.Entry<String, Object> entry : ((Map<String, Object>) this.value).entrySet()) {

			if (i == index) {
				clone.put(key, value);
			}
			if (!entry.getKey().equals(key)) {
				clone.put(entry.getKey(), entry.getValue());
			}
			i++;
		}
		if (i <= index) {
			clone.put(key, value);
		}
		return clone;

	}

	@SuppressWarnings({ "rawtypes" })
	protected List<Object> values() {

		if (value instanceof List) {
			return new ArrayList<Object>((List) value);
		}

		if (value instanceof java.util.Map) {
			return new ArrayList<Object>(((java.util.Map) value).values());
		}

		return new ArrayList<>(Collections.singletonList(value));
	}

	/**
	 * Get the value at a given index.
	 *
	 * @param index
	 * @param <T>
	 * @return
	 * @since 2.1
	 */
	@SuppressWarnings("unchecked")
	protected <T> T get(int index) {
		return (T) values().get(index);
	}

	/**
	 * Get the value for a given key.
	 *
	 * @param key
	 * @param <T>
	 * @return
	 * @since 2.1
	 */
	@SuppressWarnings("unchecked")
	protected <T> T get(Object key) {

		Assert.isInstanceOf(Map.class, this.value, "Value must be a type of Map");

		return (T) ((Map<String, Object>) this.value).get(key);
	}

	protected boolean isArgumentMap() {
		return this.value instanceof Map;
	}

	/**
	 * Get the argument map.
	 *
	 * @since 2.1
	 * @return
	 */
	@SuppressWarnings("unchecked")
	protected Map<String, Object> argumentMap() {

		Assert.isInstanceOf(Map.class, this.value, "Value must be a type of Map");

		return Collections.unmodifiableMap((java.util.Map<String, Object>) value);
	}

	/**
	 * Check if the given key is available.
	 *
	 * @param key
	 * @return
	 * @since 2.1
	 */
	@SuppressWarnings("unchecked")
	protected boolean contains(Object key) {

		if (!(this.value instanceof java.util.Map)) {
			return false;
		}

		return ((Map<String, Object>) this.value).containsKey(key);
	}

	protected abstract String getMongoMethod();
}
