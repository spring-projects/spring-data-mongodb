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
package org.springframework.data.mongodb.repository.query;

import reactor.core.publisher.Mono;

import org.bson.Document;
import org.reactivestreams.Publisher;

import org.springframework.data.mongodb.InvalidMongoDbApiUsageException;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.VectorSearch;
import org.springframework.data.mongodb.util.json.ParameterBindingContext;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.data.repository.query.ValueExpressionDelegate;
import org.springframework.data.spel.ExpressionDependencies;

/**
 * {@link AbstractReactiveMongoQuery} implementation to run a {@link VectorSearchAggregation}. The pre-filter is either
 * derived from the method name or provided through {@link VectorSearch#filter()}.
 *
 * @author Mark Paluch
 * @since 5.0
 */
public class ReactiveVectorSearchAggregation extends AbstractReactiveMongoQuery {

	private final ReactiveMongoOperations mongoOperations;
	private final MongoPersistentEntity<?> collectionEntity;
	private final ValueExpressionDelegate valueExpressionDelegate;
	private final VectorSearchDelegate delegate;

	/**
	 * Creates a new {@link ReactiveVectorSearchAggregation} from the given {@link MongoQueryMethod} and
	 * {@link MongoOperations}.
	 *
	 * @param method must not be {@literal null}.
	 * @param mongoOperations must not be {@literal null}.
	 * @param delegate must not be {@literal null}.
	 */
	public ReactiveVectorSearchAggregation(ReactiveMongoQueryMethod method, ReactiveMongoOperations mongoOperations,
			ValueExpressionDelegate delegate) {

		super(method, mongoOperations, delegate);

		this.valueExpressionDelegate = delegate;
		if (!method.isSearchQuery() && !method.isCollectionQuery()) {
			throw new InvalidMongoDbApiUsageException(String.format(
					"Repository Vector Search method '%s' must return either return SearchResults<T> or List<T> but was %s",
					method.getName(), method.getReturnType().getType().getSimpleName()));
		}

		this.mongoOperations = mongoOperations;
		this.collectionEntity = method.getEntityInformation().getCollectionEntity();
		this.delegate = new VectorSearchDelegate(method, mongoOperations.getConverter(), delegate);
	}

	@Override
	protected Publisher<Object> doExecute(ReactiveMongoQueryMethod method, ResultProcessor processor,
			ConvertingParameterAccessor accessor, @org.jspecify.annotations.Nullable Class<?> typeToRead) {

		return getParameterBindingCodec().flatMapMany(codec -> {

			String json = delegate.getQueryString();
			ExpressionDependencies dependencies = codec.captureExpressionDependencies(json, accessor::getBindableValue,
					valueExpressionDelegate);

			return getValueExpressionEvaluatorLater(dependencies, accessor).flatMapMany(expressionEvaluator -> {

				ParameterBindingContext bindingContext = new ParameterBindingContext(accessor::getBindableValue,
						expressionEvaluator);
				VectorSearchDelegate.QueryMetadata query = delegate.createQuery(expressionEvaluator, processor, accessor,
						typeToRead, codec, bindingContext);

				ReactiveMongoQueryExecution.VectorSearchExecution execution = new ReactiveMongoQueryExecution.VectorSearchExecution(
						mongoOperations, method, query, accessor);

				return execution.execute(query.query(), Document.class, collectionEntity.getCollection());
			});
		});
	}

	@Override
	protected Mono<Query> createQuery(ConvertingParameterAccessor accessor) {
		throw new UnsupportedOperationException();
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
