/*
 * Copyright 2019-2024 the original author or authors.
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
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationPipeline;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoSimpleTypes;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.repository.query.ReactiveQueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.data.repository.query.ValueExpressionDelegate;
import org.springframework.data.util.ReflectionUtils;
import org.springframework.expression.ExpressionParser;
import org.springframework.lang.Nullable;

/**
 * A reactive {@link org.springframework.data.repository.query.RepositoryQuery} to use a plain JSON String to create an
 * {@link AggregationOperation aggregation} pipeline to actually execute.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.2
 */
public class ReactiveStringBasedAggregation extends AbstractReactiveMongoQuery {

	private final ReactiveMongoOperations reactiveMongoOperations;
	private final MongoConverter mongoConverter;

	/**
	 * @param method must not be {@literal null}.
	 * @param reactiveMongoOperations must not be {@literal null}.
	 * @param expressionParser must not be {@literal null}.
	 * @param evaluationContextProvider must not be {@literal null}.
	 * @deprecated since 4.4.0, use the constructors accepting {@link ValueExpressionDelegate} instead.
	 */
	@Deprecated(since = "4.4.0")
	public ReactiveStringBasedAggregation(ReactiveMongoQueryMethod method,
			ReactiveMongoOperations reactiveMongoOperations, ExpressionParser expressionParser,
			ReactiveQueryMethodEvaluationContextProvider evaluationContextProvider) {

		super(method, reactiveMongoOperations, expressionParser, evaluationContextProvider);

		this.reactiveMongoOperations = reactiveMongoOperations;
		this.mongoConverter = reactiveMongoOperations.getConverter();
	}

	/**
	 * @param method must not be {@literal null}.
	 * @param reactiveMongoOperations must not be {@literal null}.
	 * @param delegate must not be {@literal null}.
	 * @since 4.4.0
	 */
	public ReactiveStringBasedAggregation(ReactiveMongoQueryMethod method,
			ReactiveMongoOperations reactiveMongoOperations, ValueExpressionDelegate delegate) {

		super(method, reactiveMongoOperations, delegate);

		this.reactiveMongoOperations = reactiveMongoOperations;
		this.mongoConverter = reactiveMongoOperations.getConverter();
	}

	@Override
	@SuppressWarnings("ReactiveStreamsNullableInLambdaInTransform")
	protected Publisher<Object> doExecute(ReactiveMongoQueryMethod method, ResultProcessor processor,
			ConvertingParameterAccessor accessor, @Nullable Class<?> ignored) {

		return computePipeline(accessor).flatMapMany(it -> {

			return AggregationUtils.doAggregate(new AggregationPipeline(it), method, processor, accessor,
					this::getValueExpressionEvaluator,
					(aggregation, sourceType, typeToRead, elementType, simpleType, rawResult) -> {

						Flux<?> flux = reactiveMongoOperations.aggregate(aggregation, typeToRead);
						if (ReflectionUtils.isVoid(elementType)) {
							return flux.then();
						}

						ReactiveMongoQueryExecution.ResultProcessingConverter resultProcessing = getResultProcessing(processor);

						if (simpleType && !rawResult && !elementType.equals(Document.class)) {

							flux = flux.handle((item, sink) -> {

								Object result = AggregationUtils.extractSimpleTypeResult((Document) item, elementType, mongoConverter);

								if (result != null) {
									sink.next(result);
								}
							});
						}

						flux = flux.map(resultProcessing::convert);

						return method.isCollectionQuery() ? flux : flux.next();
					});
		});
	}

	private boolean isSimpleReturnType(Class<?> targetType) {
		return MongoSimpleTypes.HOLDER.isSimpleType(targetType);
	}

	private Mono<List<AggregationOperation>> computePipeline(ConvertingParameterAccessor accessor) {
		return parseAggregationPipeline(getQueryMethod().getAnnotatedAggregation(), accessor);
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
