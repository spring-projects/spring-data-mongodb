/*
 * Copyright 2020-2023 the original author or authors.
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
import java.util.List;
import java.util.function.Predicate;

import org.bson.Document;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * The {@link AggregationPipeline} holds the collection of {@link AggregationStage aggregation stages}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 3.0.2
 */
public class AggregationPipeline {

	private final List<AggregationStage> pipeline;

	public static AggregationPipeline of(AggregationOperation... stages) {
		return new AggregationPipeline(Arrays.asList(stages));
	}

	/**
	 * Create a new {@link AggregationPipeline} out of the given {@link AggregationStage stages}.
	 *
	 * @param stages the pipeline stages.
	 * @return new instance of {@link AggregationPipeline}.
	 * @since 4.1
	 */
	public static AggregationPipeline of(AggregationStage... stages) {
		return new AggregationPipeline(Arrays.asList(stages));
	}

	/**
	 * Create an empty pipeline
	 */
	public AggregationPipeline() {
		this(new ArrayList<>());
	}

	/**
	 * Create a new pipeline with given {@link AggregationOperation stages}.
	 *
	 * @param aggregationStages must not be {@literal null}.
	 */
	public AggregationPipeline(List<? extends AggregationStage> aggregationStages) {

		Assert.notNull(aggregationStages, "AggregationStages must not be null");
		pipeline = new ArrayList<>(aggregationStages);
	}

	/**
	 * Append the given {@link AggregationOperation stage} to the pipeline.
	 *
	 * @param aggregationOperation must not be {@literal null}.
	 * @return this.
	 */
	public AggregationPipeline add(AggregationOperation aggregationOperation) {
		return add((AggregationStage) aggregationOperation);
	}

	/**
	 * Append the given {@link AggregationOperation stage} to the pipeline.
	 *
	 * @param stage must not be {@literal null}.
	 * @return this.
	 * @since 4.1
	 */
	public AggregationPipeline add(AggregationStage stage) {

		Assert.notNull(stage, "AggregationOperation must not be null");

		pipeline.add(stage);
		return this;
	}

	/**
	 * Get the list of {@link AggregationOperation aggregation stages}.
	 *
	 * @return never {@literal null}.
	 */
	public List<AggregationStage> getOperations() {
		return getStages();
	}

	/**
	 * Get the list of {@link AggregationOperation aggregation stages}.
	 *
	 * @return never {@literal null}.
	 * @since 4.1
	 */
	public List<AggregationStage> getStages() {
		return Collections.unmodifiableList(pipeline);
	}

	List<Document> toDocuments(AggregationOperationContext context) {

		verify();
		return AggregationOperationRenderer.toDocument(pipeline, context);
	}

	/**
	 * @return {@literal true} if the last aggregation stage is either {@literal $out} or {@literal $merge}.
	 */
	public boolean isOutOrMerge() {

		if (isEmpty()) {
			return false;
		}

		AggregationStage operation = pipeline.get(pipeline.size() - 1);
		return isOut(operation) || isMerge(operation);
	}

	void verify() {

		// check $out/$merge is the last operation if it exists
		for (AggregationStage operation : pipeline) {

			if (isOut(operation) && !isLast(operation)) {
				throw new IllegalArgumentException("The $out operator must be the last stage in the pipeline");
			}

			if (isMerge(operation) && !isLast(operation)) {
				throw new IllegalArgumentException("The $merge operator must be the last stage in the pipeline");
			}
		}
	}

	/**
	 * Return whether this aggregation pipeline defines a {@code $unionWith} stage that may contribute documents from
	 * other collections. Checking for presence of union stages is useful when attempting to determine the aggregation
	 * element type for mapping metadata computation.
	 *
	 * @return {@literal true} the aggregation pipeline makes use of {@code $unionWith}.
	 * @since 3.1
	 */
	public boolean containsUnionWith() {
		return containsOperation(AggregationPipeline::isUnionWith);
	}

	/**
	 * @return {@literal true} if the pipeline does not contain any stages.
	 * @since 3.1
	 */
	public boolean isEmpty() {
		return pipeline.isEmpty();
	}

	private boolean containsOperation(Predicate<AggregationStage> predicate) {

		if (isEmpty()) {
			return false;
		}

		for (AggregationStage element : pipeline) {
			if (predicate.test(element)) {
				return true;
			}
		}

		return false;
	}

	private boolean isLast(AggregationStage aggregationOperation) {
		return pipeline.indexOf(aggregationOperation) == pipeline.size() - 1;
	}

	private static boolean isUnionWith(AggregationStage stage) {
		return isSpecificStage(stage, UnionWithOperation.class, "$unionWith");
	}

	private static boolean isMerge(AggregationStage stage) {
		return isSpecificStage(stage, MergeOperation.class, "$merge");
	}

	private static boolean isOut(AggregationStage stage) {
		return isSpecificStage(stage, OutOperation.class, "$out");
	}

	private static boolean isSpecificStage(AggregationStage stage, Class<?> type, String operator) {
		if (ClassUtils.isAssignable(type, stage.getClass())) {
			return true;
		}
		if (stage instanceof AggregationOperation operation) {
			return operation.getOperator().equals(operator);
		}
		return stage.toDocument(Aggregation.DEFAULT_CONTEXT).keySet().iterator().next().equals(operator);
	}
}
