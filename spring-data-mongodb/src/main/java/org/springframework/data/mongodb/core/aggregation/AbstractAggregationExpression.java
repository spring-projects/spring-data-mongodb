/*
 * Copyright 2016. the original author or authors.
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

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.springframework.util.ObjectUtils;

/**
 * @author Christoph Strobl
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

	public DBObject toDbObject(Object value, AggregationOperationContext context) {

		Object valueToUse;
		if (value instanceof List) {

			List<Object> arguments = (List<Object>) value;
			List<Object> args = new ArrayList<Object>(arguments.size());

			for (Object val : arguments) {
				args.add(unpack(val, context));
			}
			valueToUse = args;
		} else if (value instanceof java.util.Map) {

			DBObject dbo = new BasicDBObject();
			for (java.util.Map.Entry<String, Object> entry : ((java.util.Map<String, Object>) value).entrySet()) {
				dbo.put(entry.getKey(), unpack(entry.getValue(), context));
			}
			valueToUse = dbo;
		} else {
			valueToUse = unpack(value, context);
		}

		return new BasicDBObject(getMongoMethod(), valueToUse);
	}

	protected static List<Field> asFields(String... fieldRefs) {

		if (ObjectUtils.isEmpty(fieldRefs)) {
			return Collections.emptyList();
		}

		return Fields.fields(fieldRefs).asList();
	}

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

	protected java.util.Map<String, Object> append(String key, Object value) {

		if (!(this.value instanceof java.util.Map)) {
			throw new IllegalArgumentException("o_O");
		}
		java.util.Map<String, Object> clone = new LinkedHashMap<String, Object>(
				(java.util.Map<String, Object>) this.value);
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
		return new ArrayList<Object>(Arrays.asList(value));
	}

	protected abstract String getMongoMethod();
}
