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

import org.jspecify.annotations.Nullable;
import org.springframework.data.mapping.model.ValueExpressionEvaluator;
import org.springframework.data.mongodb.InvalidMongoDbApiUsageException;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.VectorSearch;
import org.springframework.data.mongodb.repository.query.VectorSearchDelegate.QueryContainer;
import org.springframework.data.mongodb.util.json.ParameterBindingContext;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.data.repository.query.ValueExpressionDelegate;

/**
 * {@link AbstractMongoQuery} implementation to run a {@link VectorSearchAggregation}. The pre-filter is either derived
 * from the method name or provided through {@link VectorSearch#filter()}.
 *
 * @author Mark Paluch
 * @since 5.0
 */
public class VectorSearchAggregation extends AbstractMongoQuery {

	private final MongoOperations mongoOperations;
	private final MongoPersistentEntity<?> collectionEntity;
	private final VectorSearchDelegate delegate;

	/**
	 * Creates a new {@link VectorSearchAggregation} from the given {@link MongoQueryMethod} and {@link MongoOperations}.
	 *
	 * @param method must not be {@literal null}.
	 * @param mongoOperations must not be {@literal null}.
	 * @param delegate must not be {@literal null}.
	 */
	public VectorSearchAggregation(MongoQueryMethod method, MongoOperations mongoOperations,
			ValueExpressionDelegate delegate) {

		super(method, mongoOperations, delegate);

		if (!method.isSearchQuery() && !method.isCollectionQuery()) {
			throw new InvalidMongoDbApiUsageException(String.format(
					"Repository Vector Search method '%s' must return either return SearchResults<T> or List<T> but was %s",
					method.getName(), method.getReturnType().getType().getSimpleName()));
		}

		this.mongoOperations = mongoOperations;
		this.collectionEntity = method.getEntityInformation().getCollectionEntity();
		this.delegate = new VectorSearchDelegate(method, mongoOperations.getConverter(), delegate);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Object doExecute(MongoQueryMethod method, ResultProcessor processor, ConvertingParameterAccessor accessor,
			@Nullable Class<?> typeToRead) {

		QueryContainer query = createVectorSearchQuery(processor, accessor, typeToRead);

		MongoQueryExecution.VectorSearchExecution execution = new MongoQueryExecution.VectorSearchExecution(mongoOperations,
				method, collectionEntity.getCollection(), query);

		return execution.execute(query.query());
	}

	QueryContainer createVectorSearchQuery(ResultProcessor processor, MongoParameterAccessor accessor,
			@Nullable Class<?> typeToRead) {

		ValueExpressionEvaluator evaluator = getExpressionEvaluatorFor(accessor);
		ParameterBindingContext bindingContext = prepareBindingContext(delegate.getQueryString(), accessor);

		return delegate.createQuery(evaluator, processor, accessor, typeToRead, getParameterBindingCodec(), bindingContext);
	}

	@Override
	protected Query createQuery(ConvertingParameterAccessor accessor) {
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
