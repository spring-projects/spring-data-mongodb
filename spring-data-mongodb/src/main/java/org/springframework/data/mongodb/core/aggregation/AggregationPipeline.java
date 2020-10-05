/*
 * Copyright 2020 the original author or authors.
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
import java.util.Collections;
import java.util.List;

import org.bson.Document;
import org.springframework.util.Assert;

/**
 * The {@link AggregationPipeline} holds the collection of {@link AggregationOperation aggregation stages}.
 *
 * @author Christoph Strobl
 * @since 3.0.2
 */
public class AggregationPipeline {

	private final List<AggregationOperation> pipeline;

	/**
	 * Create an empty pipeline
	 */
	public AggregationPipeline() {
		this(new ArrayList<>());
	}

	/**
	 * Create a new pipeline with given {@link AggregationOperation stages}.
	 *
	 * @param aggregationOperations must not be {@literal null}.
	 */
	public AggregationPipeline(List<AggregationOperation> aggregationOperations) {
		pipeline = new ArrayList<>(aggregationOperations);
	}

	/**
	 * Append the given {@link AggregationOperation stage} to the pipeline.
	 *
	 * @param aggregationOperation must not be {@literal null}.
	 * @return this.
	 */
	public AggregationPipeline add(AggregationOperation aggregationOperation) {

		Assert.notNull(aggregationOperation, "AggregationOperation must not be null!");

		pipeline.add(aggregationOperation);
		return this;
	}

	/**
	 * Get the list of {@link AggregationOperation aggregation stages}.
	 *
	 * @return never {@literal null}.
	 */
	public List<AggregationOperation> getOperations() {
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

		if (pipeline.isEmpty()) {
			return false;
		}

		String operator = pipeline.get(pipeline.size() - 1).getOperator();
		return operator.equals("$out") || operator.equals("$merge");
	}

	void verify() {

		// check $out/$merge is the last operation if it exists
		for (AggregationOperation aggregationOperation : pipeline) {

			if (aggregationOperation instanceof OutOperation && !isLast(aggregationOperation)) {
				throw new IllegalArgumentException("The $out operator must be the last stage in the pipeline.");
			}

			if (aggregationOperation instanceof MergeOperation && !isLast(aggregationOperation)) {
				throw new IllegalArgumentException("The $merge operator must be the last stage in the pipeline.");
			}
		}
	}

	private boolean isLast(AggregationOperation aggregationOperation) {
		return pipeline.indexOf(aggregationOperation) == pipeline.size() - 1;
	}
}
