/*
 * Copyright 2015 the original author or authors.
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
import java.util.List;

import org.springframework.util.Assert;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * @author Thomas Darimont
 */
public class Expressions {

	public static Expression expression(String name, Object... values) {
		return new FunctionExpression(name, values);
	}

	/**
	 * @author Thomas Darimont
	 */
	static class FunctionExpression implements Expression {

		private final String name;
		private final Object[] values;

		public FunctionExpression(String name, Object[] values) {

			Assert.hasText(name, "Name must not be null!");
			Assert.notNull(values, "Values must not be null!");

			this.name = name;
			this.values = values;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.Expression#toDbObject(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
		 */
		@Override
		public DBObject toDbObject(AggregationOperationContext context) {

			List<Object> args = new ArrayList<Object>(values.length);
			for (int i = 0; i < values.length; i++) {
				args.add(unpack(values[i], context));
			}

			return new BasicDBObject("$" + name, args);
		}

		private Object unpack(Object value, AggregationOperationContext context) {

			if (value instanceof Expression) {
				return ((Expression) value).toDbObject(context);
			}

			if (value instanceof Field) {
				return context.getReference((Field) value).toString();
			}

			return value;
		}
	}
}
