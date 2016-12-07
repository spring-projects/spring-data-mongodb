/*
 * Copyright 2016 the original author or authors.
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

import org.springframework.data.mongodb.core.aggregation.ExposedFields.ExposedField;
import org.springframework.util.Assert;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Encapsulates the aggregation framework {@code $count}-operation.
 * <p>
 * We recommend to use the static factory method {@link Aggregation#count()} instead of creating instances of this
 * class directly.
 * 
 * @see https://docs.mongodb.com/manual/reference/operator/aggregation/count/#pipe._S_count
 * @author Mark Paluch
 * @since 1.10
 */
public class CountOperation implements FieldsExposingAggregationOperation {

	private final String fieldName;

	/**
	 * Creates a new {@link CountOperation} given the {@link fieldName} field name.
	 *
	 * @param asFieldName must not be {@literal null} or empty.
	 */
	public CountOperation(String fieldName) {

		Assert.hasText(fieldName, "Field name must not be null or empty!");
		this.fieldName = fieldName;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperation#toDBObject(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
	 */
	@Override
	public DBObject toDBObject(AggregationOperationContext context) {
		return new BasicDBObject("$count", fieldName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.FieldsExposingAggregationOperation#getFields()
	 */
	@Override
	public ExposedFields getFields() {
		return ExposedFields.from(new ExposedField(fieldName, true));
	}

	/**
	 * Builder for {@link CountOperation}.
	 *
	 * @author Mark Paluch
	 */
	public static class CountOperationBuilder {

		/**
		 * Returns the finally to be applied {@link CountOperation} with the given alias.
		 *
		 * @param fieldName must not be {@literal null} or empty.
		 * @return
		 */
		public CountOperation as(String fieldName) {
			return new CountOperation(fieldName);
		}
	}
}
