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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import org.bson.Document;

import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.SerializationUtils;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Abstraction for {@code db.collection.update()} using an aggregation pipeline. Aggregation pipeline updates use a more
 * expressive update statement expressing conditional updates based on current field values or updating one field using
 * the value of another field(s).
 *
 * <pre class="code">
 * AggregationUpdate update = AggregationUpdate.update().set("average")
 * 		.toValue(ArithmeticOperators.valueOf("tests").avg()).set("grade")
 * 		.toValue(ConditionalOperators
 * 				.switchCases(CaseOperator.when(Gte.valueOf("average").greaterThanEqualToValue(90)).then("A"),
 * 						CaseOperator.when(Gte.valueOf("average").greaterThanEqualToValue(80)).then("B"),
 * 						CaseOperator.when(Gte.valueOf("average").greaterThanEqualToValue(70)).then("C"),
 * 						CaseOperator.when(Gte.valueOf("average").greaterThanEqualToValue(60)).then("D"))
 * 				.defaultTo("F"));
 * </pre>
 *
 * The above sample is equivalent to the JSON update statement:
 *
 * <pre class="code">
 * db.collection.update(
 *    { },
 *    [
 *      { $set: { average : { $avg: "$tests" } } },
 *      { $set: { grade: { $switch: {
 *                            branches: [
 *                                { case: { $gte: [ "$average", 90 ] }, then: "A" },
 *                                { case: { $gte: [ "$average", 80 ] }, then: "B" },
 *                                { case: { $gte: [ "$average", 70 ] }, then: "C" },
 *                                { case: { $gte: [ "$average", 60 ] }, then: "D" }
 *                            ],
 *                            default: "F"
 *      } } } }
 *    ],
 *    { multi: true }
 * )
 * </pre>
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @see <a href=
 *      "https://docs.mongodb.com/manual/reference/method/db.collection.update/#update-with-aggregation-pipeline">MongoDB
 *      Reference Documentation</a>
 * @since 3.0
 */
public class AggregationUpdate extends Aggregation implements UpdateDefinition {

	private boolean isolated = false;
	private Set<String> keysTouched = new HashSet<>();

	/**
	 * Create new {@link AggregationUpdate}.
	 */
	protected AggregationUpdate() {
		this(new ArrayList<>());
	}

	/**
	 * Create new {@link AggregationUpdate} with the given aggregation pipeline to apply.
	 *
	 * @param pipeline must not be {@literal null}.
	 */
	protected AggregationUpdate(List<AggregationOperation> pipeline) {

		super(pipeline);

		for (AggregationOperation operation : pipeline) {
			if (operation instanceof FieldsExposingAggregationOperation) {
				((FieldsExposingAggregationOperation) operation).getFields().forEach(it -> {
					keysTouched.add(it.getName());
				});
			}
		}
	}

	/**
	 * Start defining the update pipeline to execute.
	 *
	 * @return new instance of {@link AggregationUpdate}.
	 */
	public static AggregationUpdate update() {
		return new AggregationUpdate();
	}

	/**
	 * Create a new AggregationUpdate from the given {@link AggregationOperation}s.
	 *
	 * @return new instance of {@link AggregationUpdate}.
	 */
	public static AggregationUpdate from(List<AggregationOperation> pipeline) {
		return new AggregationUpdate(pipeline);
	}

	/**
	 * Adds new fields to documents. {@code $set} outputs documents that contain all existing fields from the input
	 * documents and newly added fields.
	 *
	 * @param setOperation must not be {@literal null}.
	 * @return this.
	 * @see <a href="https://docs.mongodb.com/manual/reference/operator/aggregation/set/">$set Aggregation Reference</a>
	 */
	public AggregationUpdate set(SetOperation setOperation) {

		Assert.notNull(setOperation, "SetOperation must not be null!");

		setOperation.getFields().forEach(it -> {
			keysTouched.add(it.getName());
		});
		operations.add(setOperation);
		return this;
	}

	/**
	 * {@code $unset} removes/excludes fields from documents.
	 *
	 * @param unsetOperation must not be {@literal null}.
	 * @return this.
	 * @see <a href="https://docs.mongodb.com/manual/reference/operator/aggregation/unset/">$unset Aggregation
	 *      Reference</a>
	 */
	public AggregationUpdate unset(UnsetOperation unsetOperation) {

		Assert.notNull(unsetOperation, "UnsetOperation must not be null!");

		operations.add(unsetOperation);
		keysTouched.addAll(unsetOperation.removedFieldNames());
		return this;
	}

	/**
	 * {@code $replaceWith} replaces the input document with the specified document. The operation replaces all existing
	 * fields in the input document, including the <strong>_id</strong> field.
	 *
	 * @param replaceWithOperation
	 * @return this.
	 * @see <a href="https://docs.mongodb.com/manual/reference/operator/aggregation/replaceWith/">$replaceWith Aggregation
	 *      Reference</a>
	 */
	public AggregationUpdate replaceWith(ReplaceWithOperation replaceWithOperation) {

		Assert.notNull(replaceWithOperation, "ReplaceWithOperation must not be null!");
		operations.add(replaceWithOperation);
		return this;
	}

	/**
	 * {@code $replaceWith} replaces the input document with the value.
	 *
	 * @param value must not be {@literal null}.
	 * @return this.
	 */
	public AggregationUpdate replaceWith(Object value) {

		Assert.notNull(value, "Value must not be null!");
		return replaceWith(ReplaceWithOperation.replaceWithValue(value));
	}

	/**
	 * Fluent API variant for {@code $set} adding a single {@link SetOperation pipeline operation} every time. To update
	 * multiple fields within one {@link SetOperation} use {@link #set(SetOperation)}.
	 *
	 * @param key must not be {@literal null}.
	 * @return new instance of {@link SetValueAppender}.
	 * @see #set(SetOperation)
	 */
	public SetValueAppender set(String key) {

		Assert.notNull(key, "Key must not be null!");

		return new SetValueAppender() {

			@Override
			public AggregationUpdate toValue(@Nullable Object value) {
				return set(SetOperation.builder().set(key).toValue(value));
			}

			@Override
			public AggregationUpdate toValueOf(Object value) {

				Assert.notNull(value, "Value must not be null!");
				return set(SetOperation.builder().set(key).toValueOf(value));
			}
		};
	}

	/**
	 * Short for {@link #unset(UnsetOperation)}.
	 *
	 * @param keys
	 * @return
	 */
	public AggregationUpdate unset(String... keys) {

		Assert.notNull(keys, "Keys must not be null!");
		Assert.noNullElements(keys, "Keys must not contain null elements.");

		return unset(new UnsetOperation(Arrays.stream(keys).map(Fields::field).collect(Collectors.toList())));
	}

	/**
	 * Prevents a write operation that affects <strong>multiple</strong> documents from yielding to other reads or writes
	 * once the first document is written. <br />
	 * Use with {@link org.springframework.data.mongodb.core.MongoOperations#updateMulti(Query, UpdateDefinition, Class)}.
	 *
	 * @return never {@literal null}.
	 */
	public AggregationUpdate isolated() {

		isolated = true;
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.query.UpdateDefinition#isIsolated()
	 */
	@Override
	public Boolean isIsolated() {
		return isolated;
	}

	/*
	 * Returns a update document containing the update pipeline.
	 * The resulting document needs to be unwrapped to be used with update operations.
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.query.UpdateDefinition#getUpdateObject()
	 */
	@Override
	public Document getUpdateObject() {
		return new Document("", toPipeline(Aggregation.DEFAULT_CONTEXT));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.query.UpdateDefinition#modifies(java.lang.String)
	 */
	@Override
	public boolean modifies(String key) {
		return keysTouched.contains(key);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.query.UpdateDefinition#inc(java.lang.String)
	 */
	@Override
	public void inc(String key) {
		set(new SetOperation(key, ArithmeticOperators.valueOf(key).add(1)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.query.UpdateDefinition#getArrayFilters()
	 */
	@Override
	public List<ArrayFilter> getArrayFilters() {
		return Collections.emptyList();
	}

	@Override
	public String toString() {

		StringJoiner joiner = new StringJoiner(",\n", "[\n", "\n]");
		toPipeline(Aggregation.DEFAULT_CONTEXT).stream().map(SerializationUtils::serializeToJsonSafely)
				.forEach(joiner::add);
		return joiner.toString();
	}

	/**
	 * Fluent API AggregationUpdate builder.
	 *
	 * @author Christoph Strobl
	 */
	public interface SetValueAppender {

		/**
		 * Define the target value as is.
		 *
		 * @param value can be {@literal null}.
		 * @return never {@literal null}.
		 */
		AggregationUpdate toValue(@Nullable Object value);

		/**
		 * Define the target value as value, an {@link AggregationExpression} or a {@link Field} reference.
		 *
		 * @param value can be {@literal null}.
		 * @return never {@literal null}.
		 */
		AggregationUpdate toValueOf(Object value);
	}
}
