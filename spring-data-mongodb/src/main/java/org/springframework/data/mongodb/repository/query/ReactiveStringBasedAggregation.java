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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

import org.bson.Document;
import org.reactivestreams.Publisher;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationOptions;
import org.springframework.data.mongodb.core.aggregation.AggregationPipeline;
import org.springframework.data.mongodb.core.aggregation.TypedAggregation;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoSimpleTypes;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.repository.query.ReactiveQueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.data.util.ReflectionUtils;
import org.springframework.data.util.TypeInformation;
import org.springframework.expression.ExpressionParser;
import org.springframework.util.ClassUtils;

/**
 * A reactive {@link org.springframework.data.repository.query.RepositoryQuery} to use a plain JSON String to create an
 * {@link AggregationOperation aggregation} pipeline to actually execute.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.2
 */
public class ReactiveStringBasedAggregation extends AbstractReactiveMongoQuery {

	private final ExpressionParser expressionParser;
	private final ReactiveQueryMethodEvaluationContextProvider evaluationContextProvider;
	private final ReactiveMongoOperations reactiveMongoOperations;
	private final MongoConverter mongoConverter;

	/**
	 * @param method must not be {@literal null}.
	 * @param reactiveMongoOperations must not be {@literal null}.
	 * @param expressionParser must not be {@literal null}.
	 * @param evaluationContextProvider must not be {@literal null}.
	 */
	public ReactiveStringBasedAggregation(ReactiveMongoQueryMethod method,
			ReactiveMongoOperations reactiveMongoOperations, ExpressionParser expressionParser,
			ReactiveQueryMethodEvaluationContextProvider evaluationContextProvider) {

		super(method, reactiveMongoOperations, expressionParser, evaluationContextProvider);

		this.reactiveMongoOperations = reactiveMongoOperations;
		this.mongoConverter = reactiveMongoOperations.getConverter();
		this.expressionParser = expressionParser;
		this.evaluationContextProvider = evaluationContextProvider;
	}

	@Override
	protected Publisher<Object> doExecute(ReactiveMongoQueryMethod method, ResultProcessor processor,
			ConvertingParameterAccessor accessor, Class<?> typeToRead) {

		return computePipeline(accessor).flatMapMany(it -> {

			Class<?> sourceType = method.getDomainClass();
			Class<?> targetType = typeToRead;

			AggregationPipeline pipeline = new AggregationPipeline(it);

			AggregationUtils.appendSortIfPresent(pipeline, accessor, typeToRead);
			AggregationUtils.appendLimitAndOffsetIfPresent(pipeline, accessor);

			boolean isSimpleReturnType = isSimpleReturnType(typeToRead);
			boolean isRawReturnType = ClassUtils.isAssignable(org.bson.Document.class, typeToRead);

			if (isSimpleReturnType || isRawReturnType) {
				targetType = Document.class;
			}

			AggregationOptions options = computeOptions(method, accessor, pipeline);
			TypedAggregation<?> aggregation = new TypedAggregation<>(sourceType, pipeline.getOperations(), options);

			Flux<?> flux = reactiveMongoOperations.aggregate(aggregation, targetType);
			if (ReflectionUtils.isVoid(typeToRead)) {
				return flux.then();
			}

			if (isSimpleReturnType && !isRawReturnType) {
				flux = flux.handle((item, sink) -> {

					Object result = AggregationUtils.extractSimpleTypeResult((Document) item, typeToRead, mongoConverter);

					if (result != null) {
						sink.next(result);
					}
				});
			}

			return method.isCollectionQuery() ? flux : flux.next();
		});
	}

	private boolean isSimpleReturnType(Class<?> targetType) {
		return MongoSimpleTypes.HOLDER.isSimpleType(targetType);
	}

	private Mono<List<AggregationOperation>> computePipeline(ConvertingParameterAccessor accessor) {
		return parseAggregationPipeline(getQueryMethod().getAnnotatedAggregation(), accessor);
	}

	private AggregationOptions computeOptions(MongoQueryMethod method, ConvertingParameterAccessor accessor,
			AggregationPipeline pipeline) {

		AggregationOptions.Builder builder = Aggregation.newAggregationOptions();

		AggregationUtils.applyCollation(builder, method.getAnnotatedCollation(), accessor, method.getParameters(),
				expressionParser, evaluationContextProvider);
		AggregationUtils.applyMeta(builder, method);
		AggregationUtils.applyHint(builder, method);
		AggregationUtils.applyReadPreference(builder, method);

		TypeInformation<?> returnType = method.getReturnType();
		if (returnType.getComponentType() != null) {
			returnType = returnType.getRequiredComponentType();
		}
		if (ReflectionUtils.isVoid(returnType.getType()) && pipeline.isOutOrMerge()) {
			builder.skipOutput();
		}

		return builder.build();
	}

	@Override
	protected Mono<Query> createQuery(ConvertingParameterAccessor accessor) {
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
