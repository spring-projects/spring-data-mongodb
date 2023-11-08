/*
 * Copyright 2019-2023 the original author or authors.
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
package org.springframework.data.mongodb.repository.query;

import java.util.ArrayList;
import java.util.List;
import java.util.function.LongUnaryOperator;
import java.util.stream.Stream;

import org.bson.Document;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.mongodb.InvalidMongoDbApiUsageException;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOptions;
import org.springframework.data.mongodb.core.aggregation.AggregationPipeline;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.TypedAggregation;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoSimpleTypes;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.data.util.ReflectionUtils;
import org.springframework.expression.ExpressionParser;
import org.springframework.lang.Nullable;
import org.springframework.util.ClassUtils;

/**
 * {@link AbstractMongoQuery} implementation to run string-based aggregations using
 * {@link org.springframework.data.mongodb.repository.Aggregation}.
 *
 * @author Christoph Strobl
 * @author Divya Srivastava
 * @author Mark Paluch
 * @since 2.2
 */
public class StringBasedAggregation extends AbstractMongoQuery {

	private final MongoOperations mongoOperations;
	private final MongoConverter mongoConverter;
	private final ExpressionParser expressionParser;
	private final QueryMethodEvaluationContextProvider evaluationContextProvider;

	/**
	 * Creates a new {@link StringBasedAggregation} from the given {@link MongoQueryMethod} and {@link MongoOperations}.
	 *
	 * @param method must not be {@literal null}.
	 * @param mongoOperations must not be {@literal null}.
	 * @param expressionParser must not be {@literal null}.
	 * @param evaluationContextProvider must not be {@literal null}.
	 */
	public StringBasedAggregation(MongoQueryMethod method, MongoOperations mongoOperations,
			ExpressionParser expressionParser, QueryMethodEvaluationContextProvider evaluationContextProvider) {
		super(method, mongoOperations, expressionParser, evaluationContextProvider);

		if (method.isPageQuery()) {
			throw new InvalidMongoDbApiUsageException(String.format(
					"Repository aggregation method '%s' does not support '%s' return type; Please use 'Slice' or 'List' instead",
					method.getName(), method.getReturnType().getType().getSimpleName()));
		}

		this.mongoOperations = mongoOperations;
		this.mongoConverter = mongoOperations.getConverter();
		this.expressionParser = expressionParser;
		this.evaluationContextProvider = evaluationContextProvider;
	}

	@Override
	@Nullable
	protected Object doExecute(MongoQueryMethod method, ResultProcessor resultProcessor,
			ConvertingParameterAccessor accessor, Class<?> typeToRead) {

		Class<?> sourceType = method.getDomainClass();
		Class<?> targetType = typeToRead;

		AggregationPipeline pipeline = computePipeline(method, accessor);
		AggregationUtils.appendSortIfPresent(pipeline, accessor, typeToRead);

		if (method.isSliceQuery()) {
			AggregationUtils.appendLimitAndOffsetIfPresent(pipeline, accessor, LongUnaryOperator.identity(),
					limit -> limit + 1);
		} else {
			AggregationUtils.appendLimitAndOffsetIfPresent(pipeline, accessor);
		}

		boolean isSimpleReturnType = isSimpleReturnType(typeToRead);
		boolean isRawAggregationResult = ClassUtils.isAssignable(AggregationResults.class, typeToRead);

		if (isSimpleReturnType) {
			targetType = Document.class;
		} else if (isRawAggregationResult) {

			// 🙈
			targetType = method.getReturnType().getRequiredActualType().getRequiredComponentType().getType();
		}

		AggregationOptions options = computeOptions(method, accessor, pipeline);
		TypedAggregation<?> aggregation = new TypedAggregation<>(sourceType, pipeline.getOperations(), options);

		if (method.isStreamQuery()) {

			Stream<?> stream = mongoOperations.aggregateStream(aggregation, targetType);

			if (isSimpleReturnType) {
				return stream.map(it -> AggregationUtils.extractSimpleTypeResult((Document) it, typeToRead, mongoConverter));
			}

			return stream;
		}

		AggregationResults<Object> result = (AggregationResults<Object>) mongoOperations.aggregate(aggregation, targetType);
		if (ReflectionUtils.isVoid(typeToRead)) {
			return null;
		}

		if (isRawAggregationResult) {
			return result;
		}

		List<Object> results = result.getMappedResults();
		if (method.isCollectionQuery()) {
			return isSimpleReturnType ? convertResults(typeToRead, results) : results;
		}

		if (method.isSliceQuery()) {

			Pageable pageable = accessor.getPageable();
			int pageSize = pageable.getPageSize();
			List<Object> resultsToUse = isSimpleReturnType ? convertResults(typeToRead, results) : results;
			boolean hasNext = resultsToUse.size() > pageSize;
			return new SliceImpl<>(hasNext ? resultsToUse.subList(0, pageSize) : resultsToUse, pageable, hasNext);
		}

		Object uniqueResult = result.getUniqueMappedResult();

		return isSimpleReturnType
				? AggregationUtils.extractSimpleTypeResult((Document) uniqueResult, typeToRead, mongoConverter)
				: uniqueResult;
	}

	private List<Object> convertResults(Class<?> typeToRead, List<Object> mappedResults) {

		List<Object> list = new ArrayList<>(mappedResults.size());
		for (Object it : mappedResults) {
			Object extractSimpleTypeResult = AggregationUtils.extractSimpleTypeResult((Document) it, typeToRead,
					mongoConverter);
			list.add(extractSimpleTypeResult);
		}
		return list;
	}

	private boolean isSimpleReturnType(Class<?> targetType) {
		return MongoSimpleTypes.HOLDER.isSimpleType(targetType);
	}

	AggregationPipeline computePipeline(MongoQueryMethod method, ConvertingParameterAccessor accessor) {
		return new AggregationPipeline(parseAggregationPipeline(method.getAnnotatedAggregation(), accessor));
	}

	private AggregationOptions computeOptions(MongoQueryMethod method, ConvertingParameterAccessor accessor,
			AggregationPipeline pipeline) {

		AggregationOptions.Builder builder = Aggregation.newAggregationOptions();

		AggregationUtils.applyCollation(builder, method.getAnnotatedCollation(), accessor, method.getParameters(),
				expressionParser, evaluationContextProvider);
		AggregationUtils.applyMeta(builder, method);
		AggregationUtils.applyHint(builder, method);
		AggregationUtils.applyReadPreference(builder, method);

		if (ReflectionUtils.isVoid(method.getReturnType().getType()) && pipeline.isOutOrMerge()) {
			builder.skipOutput();
		}

		return builder.build();
	}

	@Override
	protected Query createQuery(ConvertingParameterAccessor accessor) {
		throw new UnsupportedOperationException("No query support for aggregation");
	}

	@Override
	protected boolean isCountQuery() {
		return false;
	}

	@Override
	protected boolean isExistsQuery() {
		return false;
	}

	@Override
	protected boolean isDeleteQuery() {
		return false;
	}

	@Override
	protected boolean isLimiting() {
		return false;
	}
}
