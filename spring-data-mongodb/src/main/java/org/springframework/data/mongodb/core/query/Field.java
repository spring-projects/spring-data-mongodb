/*
 * Copyright 2010-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.query;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.bson.Document;
import org.springframework.data.mongodb.MongoExpression;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Field projection.
 *
 * @author Thomas Risberg
 * @author Oliver Gierke
 * @author Patryk Wasik
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Owen Q
 */
public class Field {

	private final Map<String, Object> criteria = new HashMap<>();
	private final Map<String, Object> slices = new HashMap<>();
	private final Map<String, Criteria> elemMatches = new HashMap<>();
	private @Nullable String positionKey;
	private int positionValue;

	/**
	 * Include a single {@code field} to be returned by the query operation.
	 *
	 * @param field the document field name to be included.
	 * @return {@code this} field projection instance.
	 */
	public Field include(String field) {

		Assert.notNull(field, "Key must not be null");

		criteria.put(field, 1);

		return this;
	}

	/**
	 * Project a given {@link MongoExpression} to a {@link FieldProjectionExpression#as(String) field} included in the
	 * result.
	 *
	 * <pre class="code">
	 *
	 * // { 'name' : { '$toUpper' : '$name' } }
	 *
	 * // native MongoDB expression
	 * .project(MongoExpression.expressionFromString("'$toUpper' : '$name'")).as("name");
	 *
	 * // Aggregation Framework expression
	 * .project(StringOperators.valueOf("name").toUpper()).as("name");
	 *
	 * // Aggregation Framework SpEL expression
	 * .project(AggregationSpELExpression.expressionOf("toUpper(name)")).as("name");
	 * </pre>
	 *
	 * @param expression must not be {@literal null}.
	 * @return new instance of {@link FieldProjectionExpression}. Define the target field name through
	 *         {@link FieldProjectionExpression#as(String) as(String)}.
	 * @since 3.2
	 */
	public FieldProjectionExpression project(MongoExpression expression) {
		return field -> Field.this.projectAs(expression, field);
	}

	/**
	 * Project a given {@link MongoExpression} to a {@link FieldProjectionExpression#as(String) field} included in the
	 * result.
	 *
	 * <pre class="code">
	 *
	 * // { 'name' : { '$toUpper' : '$name' } }
	 *
	 * // native MongoDB expression
	 * .projectAs(MongoExpression.expressionFromString("'$toUpper' : '$name'"), "name");
	 *
	 * // Aggregation Framework expression
	 * .projectAs(StringOperators.valueOf("name").toUpper(), "name");
	 *
	 * // Aggregation Framework SpEL expression
	 * .projectAs(AggregationSpELExpression.expressionOf("toUpper(name)"), "name");
	 * </pre>
	 *
	 * @param expression must not be {@literal null}.
	 * @param field the field name used in the result.
	 * @return new instance of {@link FieldProjectionExpression}.
	 * @since 3.2
	 */
	public Field projectAs(MongoExpression expression, String field) {

		criteria.put(field, expression);
		return this;
	}

	/**
	 * Include one or more {@code fields} to be returned by the query operation.
	 *
	 * @param fields the document field names to be included.
	 * @return {@code this} field projection instance.
	 * @since 3.1
	 */
	public Field include(String... fields) {

		Assert.notNull(fields, "Keys must not be null");

		for (String key : fields) {
			criteria.put(key, 1);
		}

		return this;
	}

	/**
	 * Exclude a single {@code field} from being returned by the query operation.
	 *
	 * @param field the document field name to be included.
	 * @return {@code this} field projection instance.
	 */
	public Field exclude(String field) {

		Assert.notNull(field, "Key must not be null");

		criteria.put(field, 0);

		return this;
	}

	/**
	 * Exclude one or more {@code fields} from being returned by the query operation.
	 *
	 * @param fields the document field names to be included.
	 * @return {@code this} field projection instance.
	 * @since 3.1
	 */
	public Field exclude(String... fields) {

		Assert.notNull(fields, "Keys must not be null");

		for (String key : fields) {
			criteria.put(key, 0);
		}

		return this;
	}

	/**
	 * Project a {@code $slice} of the array {@code field} using the first {@code size} elements.
	 *
	 * @param field the document field name to project, must be an array field.
	 * @param size the number of elements to include.
	 * @return {@code this} field projection instance.
	 */
	public Field slice(String field, int size) {

		Assert.notNull(field, "Key must not be null");

		slices.put(field, size);

		return this;
	}

	/**
	 * Project a {@code $slice} of the array {@code field} using the first {@code size} elements starting at
	 * {@code offset}.
	 *
	 * @param field the document field name to project, must be an array field.
	 * @param offset the offset to start at.
	 * @param size the number of elements to include.
	 * @return {@code this} field projection instance.
	 */
	public Field slice(String field, int offset, int size) {

		slices.put(field, Arrays.asList(offset, size));
		return this;
	}

	public Field elemMatch(String field, Criteria elemMatchCriteria) {

		elemMatches.put(field, elemMatchCriteria);
		return this;
	}

	/**
	 * The array field must appear in the query. Only one positional {@code $} operator can appear in the projection and
	 * only one array field can appear in the query.
	 *
	 * @param field query array field, must not be {@literal null} or empty.
	 * @param value
	 * @return {@code this} field projection instance.
	 */
	public Field position(String field, int value) {

		Assert.hasText(field, "DocumentField must not be null or empty");

		positionKey = field;
		positionValue = value;

		return this;
	}

	public Document getFieldsObject() {

		Document document = new Document(criteria);

		for (Entry<String, Object> entry : slices.entrySet()) {
			document.put(entry.getKey(), new Document("$slice", entry.getValue()));
		}

		for (Entry<String, Criteria> entry : elemMatches.entrySet()) {
			document.put(entry.getKey(), new Document("$elemMatch", entry.getValue().getCriteriaObject()));
		}

		if (positionKey != null) {
			document.put(positionKey + ".$", positionValue);
		}

		return document;
	}

	@Override
	public boolean equals(@Nullable Object o) {

		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		Field field = (Field) o;

		if (positionValue != field.positionValue) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(criteria, field.criteria)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(slices, field.slices)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(elemMatches, field.elemMatches)) {
			return false;
		}
		return ObjectUtils.nullSafeEquals(positionKey, field.positionKey);
	}

	@Override
	public int hashCode() {

		int result = ObjectUtils.nullSafeHashCode(criteria);
		result = 31 * result + ObjectUtils.nullSafeHashCode(slices);
		result = 31 * result + ObjectUtils.nullSafeHashCode(elemMatches);
		result = 31 * result + ObjectUtils.nullSafeHashCode(positionKey);
		result = 31 * result + positionValue;
		return result;
	}

	/**
	 * Intermediate builder part for projecting a {@link MongoExpression} to a result field.
	 *
	 * @since 3.2
	 * @author Christoph Strobl
	 */
	public interface FieldProjectionExpression {

		/**
		 * Set the name to be used in the result and return a {@link Field}.
		 *
		 * @param name must not be {@literal null}.
		 * @return the calling instance {@link Field}.
		 */
		Field as(String name);
	}
}
