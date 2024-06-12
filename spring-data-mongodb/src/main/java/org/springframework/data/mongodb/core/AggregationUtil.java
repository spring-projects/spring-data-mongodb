/*
 * Copyright 2018-2024 the original author or authors.
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

import java.util.List;

import org.bson.Document;

import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.AggregationOptions.DomainTypeMapping;
import org.springframework.data.mongodb.core.aggregation.FieldLookupPolicy;
import org.springframework.data.mongodb.core.aggregation.TypeBasedAggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.TypedAggregation;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.util.Lazy;
import org.springframework.lang.Nullable;

/**
 * Utility methods to map {@link org.springframework.data.mongodb.core.aggregation.Aggregation} pipeline definitions and
 * create type-bound {@link AggregationOperationContext}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.1
 */
class AggregationUtil {

	final QueryMapper queryMapper;
	final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
	final Lazy<AggregationOperationContext> untypedMappingContext;

	AggregationUtil(QueryMapper queryMapper,
			MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext) {

		this.queryMapper = queryMapper;
		this.mappingContext = mappingContext;
		this.untypedMappingContext = Lazy.of(() -> new TypeBasedAggregationOperationContext(Object.class, mappingContext,
				queryMapper, FieldLookupPolicy.relaxed()));
	}

	AggregationOperationContext createAggregationContext(Aggregation aggregation, @Nullable Class<?> inputType) {

		DomainTypeMapping domainTypeMapping = aggregation.getOptions().getDomainTypeMapping();

		if (domainTypeMapping == DomainTypeMapping.NONE) {
			return Aggregation.DEFAULT_CONTEXT;
		}

		FieldLookupPolicy lookupPolicy = domainTypeMapping == DomainTypeMapping.STRICT
				&& !aggregation.getPipeline().containsUnionWith() ? FieldLookupPolicy.strict() : FieldLookupPolicy.relaxed();

		if (aggregation instanceof TypedAggregation<?> ta) {
			return new TypeBasedAggregationOperationContext(ta.getInputType(), mappingContext, queryMapper, lookupPolicy);
		}

		if (inputType == null) {
			return untypedMappingContext.get();
		}

		return new TypeBasedAggregationOperationContext(inputType, mappingContext, queryMapper, lookupPolicy);
	}

	/**
	 * Extract and map the aggregation pipeline into a {@link List} of {@link Document}.
	 *
	 * @param aggregation
	 * @param context
	 * @return
	 */
	List<Document> createPipeline(Aggregation aggregation, AggregationOperationContext context) {
		return aggregation.toPipeline(context);
	}

	/**
	 * Extract the command and map the aggregation pipeline.
	 *
	 * @param aggregation
	 * @param context
	 * @return
	 */
	Document createCommand(String collection, Aggregation aggregation, AggregationOperationContext context) {
		return aggregation.toDocument(collection, context);
	}

}
