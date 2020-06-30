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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.bson.Document;
import org.reactivestreams.Publisher;

import org.springframework.data.mapping.model.SpELExpressionEvaluator;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationOptions;
import org.springframework.data.mongodb.core.aggregation.TypedAggregation;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoSimpleTypes;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.util.json.ParameterBindingContext;
import org.springframework.data.mongodb.util.json.ParameterBindingDocumentCodec;
import org.springframework.data.repository.query.ReactiveQueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.data.spel.ExpressionDependencies;
import org.springframework.expression.spel.standard.SpelExpressionParser;
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

	private static final ParameterBindingDocumentCodec CODEC = new ParameterBindingDocumentCodec();

	private final SpelExpressionParser expressionParser;
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
			ReactiveMongoOperations reactiveMongoOperations, SpelExpressionParser expressionParser,
			ReactiveQueryMethodEvaluationContextProvider evaluationContextProvider) {

		super(method, reactiveMongoOperations, expressionParser, evaluationContextProvider);

		this.reactiveMongoOperations = reactiveMongoOperations;
		this.mongoConverter = reactiveMongoOperations.getConverter();
		this.expressionParser = expressionParser;
		this.evaluationContextProvider = evaluationContextProvider;
	}

	/*
	 * (non-Javascript)
	 * @see org.springframework.data.mongodb.repository.query.AbstractReactiveMongoQuery#doExecute(org.springframework.data.mongodb.repository.query.ReactiveMongoQueryMethod, org.springframework.data.repository.query.ResultProcessor, org.springframework.data.mongodb.repository.query.ConvertingParameterAccessor, java.lang.Class)
	 */
	@Override
	protected Publisher<Object> doExecute(ReactiveMongoQueryMethod method, ResultProcessor processor,
			ConvertingParameterAccessor accessor, Class<?> typeToRead) {

		return computePipeline(accessor).flatMapMany(it -> {

			Class<?> sourceType = method.getDomainClass();
			Class<?> targetType = typeToRead;

			List<AggregationOperation> pipeline = it;

			AggregationUtils.appendSortIfPresent(pipeline, accessor, typeToRead);
			AggregationUtils.appendLimitAndOffsetIfPresent(pipeline, accessor);

			boolean isSimpleReturnType = isSimpleReturnType(typeToRead);
			boolean isRawReturnType = ClassUtils.isAssignable(org.bson.Document.class, typeToRead);

			if (isSimpleReturnType || isRawReturnType) {
				targetType = Document.class;
			}

			AggregationOptions options = computeOptions(method, accessor);
			TypedAggregation<?> aggregation = new TypedAggregation<>(sourceType, pipeline, options);

			Flux<?> flux = reactiveMongoOperations.aggregate(aggregation, targetType);

			if (isSimpleReturnType && !isRawReturnType) {
				flux = flux.handle((item, sink) -> {

					Object result = AggregationUtils.extractSimpleTypeResult((Document) item, typeToRead, mongoConverter);

					if (result != null) {
						sink.next(result);
					}
				});
			}

			if (method.isCollectionQuery()) {
				return flux;
			} else {
				return flux.next();
			}
		});
	}

	private boolean isSimpleReturnType(Class<?> targetType) {
		return MongoSimpleTypes.HOLDER.isSimpleType(targetType);
	}

	private Mono<List<AggregationOperation>> computePipeline(ConvertingParameterAccessor accessor) {

		MongoQueryMethod method = getQueryMethod();

		List<Mono<AggregationOperation>> stages = new ArrayList<>(method.getAnnotatedAggregation().length);

		for (String source : method.getAnnotatedAggregation()) {

			Optional<ExpressionDependencies> dependencies = CODEC.getExpressionDependencies(source,
					accessor::getBindableValue, expressionParser);

			Mono<SpELExpressionEvaluator> evaluator = dependencies.map(
					it -> evaluationContextProvider.getEvaluationContextLater(method.getParameters(), accessor.getValues(), it))
					.map(evaluationContext -> evaluationContext
							.map(it -> (SpELExpressionEvaluator) new DefaultSpELExpressionEvaluator(expressionParser, it)))
					.orElseGet(() -> Mono.just(NoOpExpressionEvaluator.INSTANCE));

			Mono<AggregationOperation> stage = evaluator.map(it -> {

				ParameterBindingContext bindingContext = new ParameterBindingContext(accessor::getBindableValue, it);

				return ctx -> ctx.getMappedObject(CODEC.decode(source, bindingContext), method.getDomainClass());
			});
			stages.add(stage);
		}

		return Flux.concat(stages).collectList();
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
	 * @see org.springframework.data.mongodb.repository.query.AbstractReactiveMongoQuery#createQuery(org.springframework.data.mongodb.repository.query.ConvertingParameterAccessor)
	 */
	@Override
	protected Mono<Query> createQuery(ConvertingParameterAccessor accessor) {
		throw new UnsupportedOperationException("No query support for aggregation");
	}

	/*
	 * (non-Javascript)
	 * @see org.springframework.data.mongodb.repository.query.AbstractReactiveMongoQuery#isCountQuery()
	 */
	@Override
	protected boolean isCountQuery() {
		return false;
	}

	/*
	 * (non-Javascript)
	 * @see org.springframework.data.mongodb.repository.query.AbstractReactiveMongoQuery#isExistsQuery()
	 */
	@Override
	protected boolean isExistsQuery() {
		return false;
	}

	/*
	 * (non-Javascript)
	 * @see org.springframework.data.mongodb.repository.query.AbstractReactiveMongoQuery#isDeleteQuery()
	 */
	@Override
	protected boolean isDeleteQuery() {
		return false;
	}

	/*
	 * (non-Javascript)
	 * @see org.springframework.data.mongodb.repository.query.AbstractReactiveMongoQuery#isLimiting()
	 */
	@Override
	protected boolean isLimiting() {
		return false;
	}
}
