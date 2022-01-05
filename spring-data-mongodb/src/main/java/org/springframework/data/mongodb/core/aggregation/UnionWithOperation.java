/*
 * Copyright 2020-2021 the original author or authors.
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

import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * The <a href="https://docs.mongodb.com/master/reference/operator/aggregation/unionWith/">$unionWith</a> aggregation
 * stage (available since MongoDB 4.4) performs a union of two collections by combining pipeline results, potentially
 * containing duplicates, into a single result set that is handed over to the next stage. <br />
 * In order to remove duplicates it is possible to append a {@link GroupOperation} right after
 * {@link UnionWithOperation}.
 * <br />
 * If the {@link UnionWithOperation} uses a
 * <a href="https://docs.mongodb.com/master/reference/operator/aggregation/unionWith/#unionwith-pipeline">pipeline</a>
 * to process documents, field names within the pipeline will be treated as is. In order to map domain type property
 * names to actual field names (considering potential {@link org.springframework.data.mongodb.core.mapping.Field}
 * annotations) make sure the enclosing aggregation is a {@link TypedAggregation} and provide the target type for the
 * {@code $unionWith} stage via {@link #mapFieldsTo(Class)}.
 *
 * @author Christoph Strobl
 * @see <a href="https://docs.mongodb.com/master/reference/operator/aggregation/unionWith/">Aggregation Pipeline Stage:
 *      $unionWith</a>
 * @since 3.1
 */
public class UnionWithOperation implements AggregationOperation {

	private final String collection;

	private final @Nullable AggregationPipeline pipeline;

	private final @Nullable Class<?> domainType;

	public UnionWithOperation(String collection, @Nullable AggregationPipeline pipeline, @Nullable Class<?> domainType) {

		Assert.notNull(collection, "Collection must not be null!");

		this.collection = collection;
		this.pipeline = pipeline;
		this.domainType = domainType;
	}

	/**
	 * Set the name of the collection from which pipeline results should be included in the result set.<br />
	 * The collection name is used to set the {@code coll} parameter of {@code $unionWith}.
	 *
	 * @param collection the MongoDB collection name. Must not be {@literal null}.
	 * @return new instance of {@link UnionWithOperation}.
	 * @throws IllegalArgumentException if the required argument is {@literal null}.
	 */
	public static UnionWithOperation unionWith(String collection) {
		return new UnionWithOperation(collection, null, null);
	}

	/**
	 * Set the {@link AggregationPipeline} to apply to the specified collection. The pipeline corresponds to the optional
	 * {@code pipeline} field of the {@code $unionWith} aggregation stage and is used to compute the documents going into
	 * the result set.
	 *
	 * @param pipeline the {@link AggregationPipeline} that computes the documents. Must not be {@literal null}.
	 * @return new instance of {@link UnionWithOperation}.
	 * @throws IllegalArgumentException if the required argument is {@literal null}.
	 */
	public UnionWithOperation pipeline(AggregationPipeline pipeline) {
		return new UnionWithOperation(collection, pipeline, domainType);
	}

	/**
	 * Set the aggregation pipeline stages to apply to the specified collection. The pipeline corresponds to the optional
	 * {@code pipeline} field of the {@code $unionWith} aggregation stage and is used to compute the documents going into
	 * the result set.
	 *
	 * @param aggregationStages the aggregation pipeline stages that compute the documents. Must not be {@literal null}.
	 * @return new instance of {@link UnionWithOperation}.
	 * @throws IllegalArgumentException if the required argument is {@literal null}.
	 */
	public UnionWithOperation pipeline(List<AggregationOperation> aggregationStages) {
		return new UnionWithOperation(collection, new AggregationPipeline(aggregationStages), domainType);
	}

	/**
	 * Set the aggregation pipeline stages to apply to the specified collection. The pipeline corresponds to the optional
	 * {@code pipeline} field of the {@code $unionWith} aggregation stage and is used to compute the documents going into
	 * the result set.
	 *
	 * @param aggregationStages the aggregation pipeline stages that compute the documents. Must not be {@literal null}.
	 * @return new instance of {@link UnionWithOperation}.
	 * @throws IllegalArgumentException if the required argument is {@literal null}.
	 */
	public UnionWithOperation pipeline(AggregationOperation... aggregationStages) {
		return new UnionWithOperation(collection, new AggregationPipeline(Arrays.asList(aggregationStages)), domainType);
	}

	/**
	 * Set domain type used for field name mapping of property references used by the {@link AggregationPipeline}.
	 * Remember to also use a {@link TypedAggregation} in the outer pipeline.<br />
	 * If not set, field names used within {@link AggregationOperation pipeline operations} are taken as is.
	 *
	 * @param domainType the domain type to map field names used in pipeline operations to. Must not be {@literal null}.
	 * @return new instance of {@link UnionWithOperation}.
	 * @throws IllegalArgumentException if the required argument is {@literal null}.
	 */
	public UnionWithOperation mapFieldsTo(Class<?> domainType) {

		Assert.notNull(domainType, "DomainType must not be null!");
		return new UnionWithOperation(collection, pipeline, domainType);
	}

	@Override
	public Document toDocument(AggregationOperationContext context) {

		Document $unionWith = new Document("coll", collection);
		if (pipeline == null || pipeline.isEmpty()) {
			return new Document(getOperator(), $unionWith);
		}

		$unionWith.append("pipeline", pipeline.toDocuments(computeContext(context)));
		return new Document(getOperator(), $unionWith);
	}

	private AggregationOperationContext computeContext(AggregationOperationContext source) {

		if (source instanceof TypeBasedAggregationOperationContext) {
			return ((TypeBasedAggregationOperationContext) source).continueOnMissingFieldReference(domainType != null ? domainType : Object.class);
		}

		if (source instanceof ExposedFieldsAggregationOperationContext) {
			return computeContext(((ExposedFieldsAggregationOperationContext) source).getRootContext());
		}

		return source;
	}

	@Override
	public String getOperator() {
		return "$unionWith";
	}
}
