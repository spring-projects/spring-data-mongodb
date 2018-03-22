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
import java.util.Map.Entry;

import org.springframework.util.ObjectUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

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
	 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpression#toDbObject(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
	 */
	@Override
	public DBObject toDbObject(AggregationOperationContext context) {
		return toDbObject(this.value, context);
	}

	@SuppressWarnings("unchecked")
	public DBObject toDbObject(Object value, AggregationOperationContext context) {

		return new BasicDBObject(getMongoMethod(), unpack(value, context));
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
			return ((AggregationExpression) value).toDbObject(context);
		}

		if (value instanceof Field) {
			return context.getReference((Field) value).toString();
		}

		if (value instanceof List) {

			List<Object> sourceList = (List<Object>) value;
			List<Object> mappedList = new ArrayList<Object>(sourceList.size());

			for (Object item : sourceList) {
				mappedList.add(unpack(item, context));
			}

			return mappedList;
		} else if (value instanceof java.util.Map) {

			DBObject targetDbo = new BasicDBObject();
			for (Entry<String, Object> item : ((Map<String, Object>) value).entrySet()) {
				targetDbo.put(item.getKey(), unpack(item.getValue(), context));
			}

			return targetDbo;
		}

		return value;
	}

	protected List<Object> append(Object value) {

		if (this.value instanceof List) {

			List<Object> clone = new ArrayList<Object>((List) this.value);

			if (value instanceof List) {
				for (Object val : (List) value) {
					clone.add(val);
				}
			} else {
				clone.add(value);
			}
			return clone;
		}

		return Arrays.asList(this.value, value);
	}

	@SuppressWarnings("unchecked")
	protected java.util.Map<String, Object> append(String key, Object value) {

		if (!(this.value instanceof java.util.Map)) {
			throw new IllegalArgumentException("o_O");
		}
		java.util.Map<String, Object> clone = new LinkedHashMap<String, Object>((java.util.Map) this.value);
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
		return new ArrayList<Object>(Collections.singletonList(value));
	}

	/**
	 * Get the value at a given index.
	 *
	 * @param index
	 * @param <T>
	 * @return
	 * @since 2.1
	 */
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
	protected <T> T get(Object key) {

		if (!(this.value instanceof java.util.Map)) {
			throw new IllegalArgumentException("o_O");
		}

		return (T) ((java.util.Map<String, Object>) this.value).get(key);
	}

	/**
	 * Get the argument map.
	 *
	 * @since 2.1
	 * @return
	 */
	protected java.util.Map<String, Object> argumentMap() {

		if (!(this.value instanceof java.util.Map)) {
			throw new IllegalArgumentException("o_O");
		}

		return Collections.unmodifiableMap((java.util.Map) value);
	}

	/**
	 * Check if the given key is available.
	 *
	 * @param key
	 * @return
	 * @since 2.1
	 */
	protected boolean contains(Object key) {

		if (!(this.value instanceof java.util.Map)) {
			return false;
		}

		return ((java.util.Map<String, Object>) this.value).containsKey(key);
	}

	protected abstract String getMongoMethod();
}
