/*
 * Copyright 2018-2019 the original author or authors.
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
package org.springframework.data.mongodb.core;

import lombok.AllArgsConstructor;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.bson.Document;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.TypeBasedAggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.TypedAggregation;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.lang.Nullable;
import org.springframework.util.ObjectUtils;

/**
 * Utility methods to map {@link org.springframework.data.mongodb.core.aggregation.Aggregation} pipeline definitions and
 * create type-bound {@link AggregationOperationContext}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.0.8
 */
@AllArgsConstructor
class AggregationUtil {

	QueryMapper queryMapper;
	MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;

	/**
	 * Prepare the {@link AggregationOperationContext} for a given aggregation by either returning the context itself it
	 * is not {@literal null}, create a {@link TypeBasedAggregationOperationContext} if the aggregation contains type
	 * information (is a {@link TypedAggregation}) or use the {@link Aggregation#DEFAULT_CONTEXT}.
	 *
	 * @param aggregation must not be {@literal null}.
	 * @param context can be {@literal null}.
	 * @return the root {@link AggregationOperationContext} to use.
	 */
	AggregationOperationContext prepareAggregationContext(Aggregation aggregation,
			@Nullable AggregationOperationContext context) {

		if (context != null) {
			return context;
		}

		if (aggregation instanceof TypedAggregation) {
			return new TypeBasedAggregationOperationContext(((TypedAggregation) aggregation).getInputType(), mappingContext,
					queryMapper);
		}

		return Aggregation.DEFAULT_CONTEXT;
	}

	/**
	 * Extract and map the aggregation pipeline into a {@link List} of {@link Document}.
	 *
	 * @param aggregation
	 * @param context
	 * @return
	 */
	Document createPipeline(String collectionName, Aggregation aggregation, AggregationOperationContext context) {

		if (!ObjectUtils.nullSafeEquals(context, Aggregation.DEFAULT_CONTEXT)) {
			return aggregation.toDocument(collectionName, context);
		}

		Document command = aggregation.toDocument(collectionName, context);
		command.put("pipeline", mapAggregationPipeline(command.get("pipeline", List.class)));

		return command;
	}

	/**
	 * Extract the command and map the aggregation pipeline.
	 *
	 * @param aggregation
	 * @param context
	 * @return
	 */
	Document createCommand(String collection, Aggregation aggregation, AggregationOperationContext context) {

		Document command = aggregation.toDocument(collection, context);

		if (!ObjectUtils.nullSafeEquals(context, Aggregation.DEFAULT_CONTEXT)) {
			return command;
		}

		command.put("pipeline", mapAggregationPipeline(command.get("pipeline", List.class)));

		return command;
	}

	private List<Document> mapAggregationPipeline(List<Document> pipeline) {

		return pipeline.stream().map(val -> queryMapper.getMappedObject(val, Optional.empty()))
				.collect(Collectors.toList());
	}
}
