/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.mongodb.repository.aot;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.springframework.data.mongodb.BindableMongoExpression;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationPipeline;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.mapping.FieldName;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;

/**
 * Support class for MongoDB AOT repository fragments.
 *
 * @author Christoph Strobl
 * @since 5.0
 */
public class MongoAotRepositoryFragmentSupport {

	private final RepositoryMetadata repositoryMetadata;
	private final MongoOperations mongoOperations;
	private final MongoConverter mongoConverter;
	private final ProjectionFactory projectionFactory;

	protected MongoAotRepositoryFragmentSupport(MongoOperations mongoOperations,
			RepositoryFactoryBeanSupport.FragmentCreationContext context) {
		this(mongoOperations, context.getRepositoryMetadata(), context.getProjectionFactory());
	}

	protected MongoAotRepositoryFragmentSupport(MongoOperations mongoOperations, RepositoryMetadata repositoryMetadata,
			ProjectionFactory projectionFactory) {

		this.mongoOperations = mongoOperations;
		this.mongoConverter = mongoOperations.getConverter();
		this.repositoryMetadata = repositoryMetadata;
		this.projectionFactory = projectionFactory;
	}

	protected Document bindParameters(String source, Object[] parameters) {
		return new BindableMongoExpression(source, this.mongoConverter, parameters).toDocument();
	}

	protected BasicQuery createQuery(String queryString, Object[] parameters) {

		Document queryDocument = bindParameters(queryString, parameters);
		return new BasicQuery(queryDocument);
	}

	protected AggregationPipeline createPipeline(List<Object> rawStages) {

		List<AggregationOperation> stages = new ArrayList<>(rawStages.size());
		boolean first = true;
		for (Object rawStage : rawStages) {
			if (rawStage instanceof Document stageDocument) {
				if (first) {
					stages.add((ctx) -> ctx.getMappedObject(stageDocument));
				} else {
					stages.add((ctx) -> stageDocument);
				}
			} else if (rawStage instanceof AggregationOperation aggregationOperation) {
				stages.add(aggregationOperation);
			} else {
				throw new RuntimeException("%s cannot be converted to AggregationOperation".formatted(rawStage.getClass()));
			}
			if (first) {
				first = false;
			}
		}
		return new AggregationPipeline(stages);
	}

	protected List<Object> convertSimpleRawResults(Class<?> targetType, List<Document> rawResults) {

		List<Object> list = new ArrayList<>(rawResults.size());
		for (Document it : rawResults) {
			list.add(extractSimpleTypeResult(it, targetType, mongoConverter));
		}
		return list;
	}

	protected @Nullable Object convertSimpleRawResult(Class<?> targetType, Document rawResult) {
		return extractSimpleTypeResult(rawResult, targetType, mongoConverter);
	}

	private static <T> @Nullable T extractSimpleTypeResult(@Nullable Document source, Class<T> targetType,
			MongoConverter converter) {

		if (ObjectUtils.isEmpty(source)) {
			return null;
		}

		if (source.size() == 1) {
			return getPotentiallyConvertedSimpleTypeValue(converter, source.values().iterator().next(), targetType);
		}

		Document intermediate = new Document(source);
		intermediate.remove(FieldName.ID.name());

		if (intermediate.size() == 1) {
			return getPotentiallyConvertedSimpleTypeValue(converter, intermediate.values().iterator().next(), targetType);
		}

		for (Map.Entry<String, Object> entry : intermediate.entrySet()) {
			if (entry != null && ClassUtils.isAssignable(targetType, entry.getValue().getClass())) {
				return targetType.cast(entry.getValue());
			}
		}

		throw new IllegalArgumentException(
				String.format("o_O no entry of type %s found in %s.", targetType.getSimpleName(), source.toJson()));
	}

	@Nullable
	@SuppressWarnings("unchecked")
	private static <T> T getPotentiallyConvertedSimpleTypeValue(MongoConverter converter, @Nullable Object value,
			Class<T> targetType) {

		if (value == null) {
			return null;
		}

		if (ClassUtils.isAssignableValue(targetType, value)) {
			return (T) value;
		}

		return converter.getConversionService().convert(value, targetType);
	}

}
