/*
 * Copyright 2019-2020 the original author or authors.
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

import java.util.List;
import java.util.stream.Collectors;

import org.bson.Document;

import org.springframework.data.mongodb.InvalidMongoDbApiUsageException;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationOptions;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.TypedAggregation;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoSimpleTypes;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.expression.ExpressionParser;
import org.springframework.util.ClassUtils;

/**
 * @author Christoph Strobl
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
	 * @param expressionParser
	 * @param evaluationContextProvider
	 */
	public StringBasedAggregation(MongoQueryMethod method, MongoOperations mongoOperations,
			ExpressionParser expressionParser, QueryMethodEvaluationContextProvider evaluationContextProvider) {
		super(method, mongoOperations, expressionParser, evaluationContextProvider);

		this.mongoOperations = mongoOperations;
		this.mongoConverter = mongoOperations.getConverter();
		this.expressionParser = expressionParser;
		this.evaluationContextProvider = evaluationContextProvider;
	}

	/*
	 * (non-Javascript)
	 * @see org.springframework.data.mongodb.repository.query.AbstractReactiveMongoQuery#doExecute(org.springframework.data.mongodb.repository.query.MongoQueryMethod, org.springframework.data.repository.query.ResultProcessor, org.springframework.data.mongodb.repository.query.ConvertingParameterAccessor, java.lang.Class)
	 */
	@Override
	protected Object doExecute(MongoQueryMethod method, ResultProcessor resultProcessor,
			ConvertingParameterAccessor accessor, Class<?> typeToRead) {

		if (method.isPageQuery() || method.isSliceQuery()) {
			throw new InvalidMongoDbApiUsageException(String.format("Repository aggregation method '%s' does not support '%s' return type. Please use eg. 'List' instead.", method.getName(), method.getReturnType().getType().getSimpleName()));
		}

		Class<?> sourceType = method.getDomainClass();
		Class<?> targetType = typeToRead;

		List<AggregationOperation> pipeline = computePipeline(method, accessor);
		AggregationUtils.appendSortIfPresent(pipeline, accessor, typeToRead);
		AggregationUtils.appendLimitAndOffsetIfPresent(pipeline, accessor);

		boolean isSimpleReturnType = isSimpleReturnType(typeToRead);
		boolean isRawAggregationResult = ClassUtils.isAssignable(AggregationResults.class, typeToRead);

		if (isSimpleReturnType) {
			targetType = Document.class;
		} else if (isRawAggregationResult) {
			targetType = method.getReturnType().getRequiredActualType().getRequiredComponentType().getType();
		}

		AggregationOptions options = computeOptions(method, accessor);
		TypedAggregation<?> aggregation = new TypedAggregation<>(sourceType, pipeline, options);

		AggregationResults<?> result = mongoOperations.aggregate(aggregation, targetType);

		if (isRawAggregationResult) {
			return result;
		}

		if (method.isCollectionQuery()) {

			if (isSimpleReturnType) {

				return result.getMappedResults().stream()
						.map(it -> AggregationUtils.extractSimpleTypeResult((Document) it, typeToRead, mongoConverter))
						.collect(Collectors.toList());
			}

			return result.getMappedResults();
		}

		Object uniqueResult = result.getUniqueMappedResult();

		return isSimpleReturnType
				? AggregationUtils.extractSimpleTypeResult((Document) uniqueResult, typeToRead, mongoConverter)
				: uniqueResult;
	}

	private boolean isSimpleReturnType(Class<?> targetType) {
		return MongoSimpleTypes.HOLDER.isSimpleType(targetType);
	}

	List<AggregationOperation> computePipeline(MongoQueryMethod method, ConvertingParameterAccessor accessor) {
		return AggregationUtils.computePipeline(method, accessor, expressionParser, evaluationContextProvider);
	}

	private AggregationOptions computeOptions(MongoQueryMethod method, ConvertingParameterAccessor accessor) {

		AggregationOptions.Builder builder = Aggregation.newAggregationOptions();

		AggregationUtils.applyCollation(builder, method.getAnnotatedCollation(), accessor, method.getParameters(),
				expressionParser, evaluationContextProvider);
		AggregationUtils.applyMeta(builder, method);

		return builder.build();
	}

	/*
	 * (non-Javascript)
	 * @see org.springframework.data.mongodb.repository.query.AbstractMongoQuery#createQuery(org.springframework.data.mongodb.repository.query.ConvertingParameterAccessor)
	 */
	@Override
	protected Query createQuery(ConvertingParameterAccessor accessor) {
		throw new UnsupportedOperationException("No query support for aggregation");
	}

	/*
	 * (non-Javascript)
	 * @see org.springframework.data.mongodb.repository.query.AbstractMongoQuery#isCountQuery()
	 */
	@Override
	protected boolean isCountQuery() {
		return false;
	}

	/*
	 * (non-Javascript)
	 * @see org.springframework.data.mongodb.repository.query.AbstractMongoQuery#isExistsQuery()
	 */
	@Override
	protected boolean isExistsQuery() {
		return false;
	}

	/*
	 * (non-Javascript)
	 * @see org.springframework.data.mongodb.repository.query.AbstractMongoQuery#isDeleteQuery()
	 */
	@Override
	protected boolean isDeleteQuery() {
		return false;
	}

	/*
	 * (non-Javascript)
	 * @see org.springframework.data.mongodb.repository.query.AbstractMongoQuery#isLimiting()
	 */
	@Override
	protected boolean isLimiting() {
		return false;
	}
}
