/*
 * Copyright 2018-2019 the original author or authors.
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
package org.springframework.data.mongodb.core;

import lombok.AllArgsConstructor;

import java.util.List;

import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.TypeBasedAggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.TypedAggregation;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.util.ObjectUtils;

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;

/**
 * Utility methods to map {@link org.springframework.data.mongodb.core.aggregation.Aggregation} pipeline definitions and
 * create type-bound {@link AggregationOperationContext}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.10.13
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
	AggregationOperationContext prepareAggregationContext(Aggregation aggregation, AggregationOperationContext context) {

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
	DBObject createPipeline(String collectionName, Aggregation aggregation, AggregationOperationContext context) {

		if (!ObjectUtils.nullSafeEquals(context, Aggregation.DEFAULT_CONTEXT)) {
			return aggregation.toDbObject(collectionName, context);
		}

		DBObject command = aggregation.toDbObject(collectionName, context);
		command.put("pipeline", mapAggregationPipeline((List) command.get("pipeline")));

		return command;
	}

	/**
	 * Extract the command and map the aggregation pipeline.
	 *
	 * @param aggregation
	 * @param context
	 * @return
	 */
	DBObject createCommand(String collection, Aggregation aggregation, AggregationOperationContext context) {

		DBObject command = aggregation.toDbObject(collection, context);

		if (!ObjectUtils.nullSafeEquals(context, Aggregation.DEFAULT_CONTEXT)) {
			return command;
		}

		command.put("pipeline", mapAggregationPipeline((List) command.get("pipeline")));

		return command;
	}

	private BasicDBList mapAggregationPipeline(List<DBObject> pipeline) {

		BasicDBList mapped = new BasicDBList();

		for (DBObject stage : pipeline) {
			mapped.add(queryMapper.getMappedObject(stage, null));
		}

		return mapped;
	}
}
