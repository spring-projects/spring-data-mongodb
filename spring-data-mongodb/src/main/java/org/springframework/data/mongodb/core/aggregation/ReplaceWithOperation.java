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

import java.util.Collection;
import java.util.stream.Collectors;

import org.bson.Document;
import org.springframework.util.Assert;

/**
 * Encapsulates the aggregation framework {@code $replaceRoot}-operation. <br />
 * The operation replaces all existing fields including the {@code id} field with @{code $replaceWith}. This way it is
 * possible to promote an embedded document to the top-level or specify a new document.
 *
 * @author Christoph Strobl
 * @since 3.0
 * @see <a href="https://docs.mongodb.com/manual/reference/operator/aggregation/replaceWith/">MongoDB Aggregation
 *      Framework: $replaceWith</a>
 */
public class ReplaceWithOperation extends ReplaceRootOperation {

	/**
	 * Creates new instance of {@link ReplaceWithOperation}.
	 *
	 * @param replacement must not be {@literal null}.
	 */
	public ReplaceWithOperation(Replacement replacement) {
		super(replacement);
	}

	/**
	 * Creates new instance of {@link ReplaceWithOperation}.
	 *
	 * @param value must not be {@literal null}.
	 * @return new instance of {@link ReplaceWithOperation}.
	 */
	public static ReplaceWithOperation replaceWithValue(Object value) {
		return new ReplaceWithOperation((ctx) -> value);
	}

	/**
	 * Creates new instance of {@link ReplaceWithOperation} treating a given {@link String} {@literal value} as a
	 * {@link Field field reference}.
	 *
	 * @param value must not be {@literal null}.
	 * @return
	 */
	public static ReplaceWithOperation replaceWithValueOf(Object value) {

		Assert.notNull(value, "Value must not be null!");
		return new ReplaceWithOperation((ctx) -> {

			Object target = value instanceof String ? Fields.field((String) value) : value;
			return computeValue(target, ctx);
		});
	}

	private static Object computeValue(Object value, AggregationOperationContext context) {

		if (value instanceof Field) {
			return context.getReference((Field) value).toString();
		}
		if (value instanceof AggregationExpression) {
			return ((AggregationExpression) value).toDocument(context);
		}
		if (value instanceof Collection) {
			return ((Collection) value).stream().map(it -> computeValue(it, context)).collect(Collectors.toList());
		}

		return value;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperation#toDocument(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
	 */
	@Override
	public Document toDocument(AggregationOperationContext context) {
		return context.getMappedObject(new Document("$replaceWith", getReplacement().toDocumentExpression(context)));
	}
}
