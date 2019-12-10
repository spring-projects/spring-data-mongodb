/*
 * Copyright 2019 the original author or authors.
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
import java.util.List;
import java.util.stream.Collectors;

import org.bson.Document;
import org.springframework.data.mongodb.core.aggregation.FieldsExposingAggregationOperation.InheritsFieldsAggregationOperation;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

/**
 * Removes fields from documents.
 *
 * @author Christoph Strobl
 * @since 3.0
 * @see <a href="https://docs.mongodb.com/manual/reference/operator/aggregation/unset/">MongoDB Aggregation Framework:
 *      $unset</a>
 */
public class UnsetOperation implements InheritsFieldsAggregationOperation {

	private final Collection<Object> fields;

	/**
	 * Create new instance of {@link UnsetOperation}.
	 * 
	 * @param fields must not be {@literal null}.
	 */
	public UnsetOperation(Collection<Object> fields) {

		Assert.notNull(fields, "Fields must not be null!");
		Assert.noNullElements(fields, "Fields must not contain null values.");

		this.fields = fields;
	}

	/**
	 * Create new instance of {@link UnsetOperation}.
	 *
	 * @param fields must not be {@literal null}.
	 * @return new instance of {@link UnsetOperation}.
	 */
	public static UnsetOperation unset(String... fields) {
		return new UnsetOperation(Arrays.asList(fields));
	}

	/**
	 * Also unset the given fields.
	 *
	 * @param fields must not be {@literal null}.
	 * @return new instance of {@link UnsetOperation}.
	 */
	public UnsetOperation and(String... fields) {

		List<Object> target = new ArrayList<>(this.fields);
		CollectionUtils.mergeArrayIntoCollection(fields, target);
		return new UnsetOperation(target);
	}

	/**
	 * Also unset the given fields.
	 *
	 * @param fields must not be {@literal null}.
	 * @return new instance of {@link UnsetOperation}.
	 */
	public UnsetOperation and(Field... fields) {

		List<Object> target = new ArrayList<>(this.fields);
		CollectionUtils.mergeArrayIntoCollection(fields, target);
		return new UnsetOperation(target);
	}

	/*
	 * (non-Javadoc)
	 *  @see org.springframework.data.mongodb.core.aggregation.FieldsExposingAggregationOperation#getFields()
	 */
	@Override
	public ExposedFields getFields() {
		return ExposedFields.from();
	}

	Collection<String> removedFieldNames() {

		List<String> fieldNames = new ArrayList<>(fields.size());
		for (Object it : fields) {
			if (it instanceof Field) {
				fieldNames.add(((Field) it).getName());
			} else {
				fieldNames.add(it.toString());
			}
		}
		return fieldNames;
	}

	/*
	 * (non-Javadoc)
	 *  @see org.springframework.data.mongodb.core.aggregation.AggregationOperation#toDocument(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
	 */
	@Override
	public Document toDocument(AggregationOperationContext context) {

		if (fields.size() == 1) {
			return new Document("$unset", computeFieldName(fields.iterator().next(), context));
		}

		return new Document("$unset",
				fields.stream().map(it -> computeFieldName(it, context)).collect(Collectors.toList()));
	}

	private Object computeFieldName(Object field, AggregationOperationContext context) {

		if (field instanceof Field) {
			return context.getReference((Field) field).getRaw();
		}

		if (field instanceof AggregationExpression) {
			return ((AggregationExpression) field).toDocument(context);
		}

		if (field instanceof String) {
			return context.getReference((String) field).getRaw();
		}

		return field;
	}

}
