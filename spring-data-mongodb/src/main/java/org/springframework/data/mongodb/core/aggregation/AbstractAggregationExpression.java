/*
 * Copyright 2016-2018. the original author or authors.
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
package org.springframework.data.mongodb.core.aggregation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * @author Christoph Strobl
 * @author Matt Morrissette
 * @since 1.10
 */
abstract class AbstractAggregationExpression implements AggregationExpression {

	private final Object value;

	protected AbstractAggregationExpression(Object value) {
		this.value = value;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpression#toDocument(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
	 */
	@Override
	public Document toDocument(AggregationOperationContext context) {
		return toDocument(this.value, context);
	}

	@SuppressWarnings("unchecked")
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

		if (value instanceof AggregationExpression) {
			return ((AggregationExpression) value).toDocument(context);
		}

		if (value instanceof Field) {
			return context.getReference((Field) value).toString();
		}

		if (value instanceof List) {

			List<Object> sourceList = (List<Object>) value;
			List<Object> mappedList = new ArrayList<>(sourceList.size());

			sourceList.stream().map((item) -> unpack(item, context)).forEach(mappedList::add);

			return mappedList;
		}

		if (value instanceof Map) {

			Document targetDocument = new Document();

			Map<String, Object> sourceMap = (Map<String, Object>) value;
			sourceMap.forEach((k, v) -> targetDocument.append(k, unpack(v, context)));

			return targetDocument;
		}

		return value;
	}

	protected List<Object> append(Object value) {

		if (this.value instanceof List) {

			List<Object> clone = new ArrayList<Object>((List) this.value);

			if (value instanceof List) {
				clone.addAll((List) value);
			} else {
				clone.add(value);
			}
			return clone;
		}

		return Arrays.asList(this.value, value);
	}

	@SuppressWarnings("unchecked")
	protected java.util.Map<String, Object> append(String key, Object value) {

		Assert.isInstanceOf(Map.class, this.value, "Value must be a type of Map!");

		java.util.Map<String, Object> clone = new LinkedHashMap<>((java.util.Map) this.value);
		clone.put(key, value);
		return clone;

	}

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

		Assert.isInstanceOf(Map.class, this.value, "Value must be a type of Map!");

		return (T) ((java.util.Map<String, Object>) this.value).get(key);
	}

	/**
	 * Get the argument map.
	 *
	 * @since 2.1
	 * @return
	 */
	@SuppressWarnings("unchecked")
	protected java.util.Map<String, Object> argumentMap() {

		Assert.isInstanceOf(Map.class, this.value, "Value must be a type of Map!");

		return Collections.unmodifiableMap((java.util.Map) value);
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

		return ((java.util.Map<String, Object>) this.value).containsKey(key);
	}

	protected abstract String getMongoMethod();
}
