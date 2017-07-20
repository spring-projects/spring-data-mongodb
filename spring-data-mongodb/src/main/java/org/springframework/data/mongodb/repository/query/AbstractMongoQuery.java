/*
 * Copyright 2010-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.repository.query;

import org.springframework.data.mongodb.core.ExecutableFindOperation.FindWithProjection;
import org.springframework.data.mongodb.core.ExecutableFindOperation.FindWithQuery;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.query.MongoQueryExecution.DeleteExecution;
import org.springframework.data.mongodb.repository.query.MongoQueryExecution.GeoNearExecution;
import org.springframework.data.mongodb.repository.query.MongoQueryExecution.PagedExecution;
import org.springframework.data.mongodb.repository.query.MongoQueryExecution.PagingGeoNearExecution;
import org.springframework.data.mongodb.repository.query.MongoQueryExecution.SlicedExecution;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.util.Assert;

/**
 * Base class for {@link RepositoryQuery} implementations for Mongo.
 *
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public abstract class AbstractMongoQuery implements RepositoryQuery {

	private final MongoQueryMethod method;
	private final MongoOperations operations;
	private final FindWithProjection<?> findOperationWithProjection;

	/**
	 * Creates a new {@link AbstractMongoQuery} from the given {@link MongoQueryMethod} and {@link MongoOperations}.
	 *
	 * @param method must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 */
	public AbstractMongoQuery(MongoQueryMethod method, MongoOperations operations) {

		Assert.notNull(operations, "MongoOperations must not be null!");
		Assert.notNull(method, "MongoQueryMethod must not be null!");

		this.method = method;
		this.operations = operations;

		ReturnedType returnedType = method.getResultProcessor().getReturnedType();

		this.findOperationWithProjection = operations//
				.query(returnedType.getDomainType())//
				.inCollection(method.getEntityInformation().getCollectionName());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.RepositoryQuery#getQueryMethod()
	 */
	public MongoQueryMethod getQueryMethod() {
		return method;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.RepositoryQuery#execute(java.lang.Object[])
	 */
	public Object execute(Object[] parameters) {

		ConvertingParameterAccessor accessor = new ConvertingParameterAccessor(operations.getConverter(),
				new MongoParametersParameterAccessor(method, parameters));
		Query query = createQuery(accessor);

		applyQueryMetaAttributesWhenPresent(query);

		ResultProcessor processor = method.getResultProcessor().withDynamicProjection(accessor);
		ReturnedType returnedType = processor.getReturnedType();
		FindWithQuery<?> find = findOperationWithProjection.as(returnedType.getTypeToRead());

		MongoQueryExecution execution = getExecution(accessor, find);

		return processor.processResult(execution.execute(query));
	}

	private MongoQueryExecution getExecution(ConvertingParameterAccessor accessor, FindWithQuery<?> operation) {

		if (isDeleteQuery()) {
			return new DeleteExecution(operations, method);
		} else if (method.isGeoNearQuery() && method.isPageQuery()) {
			return new PagingGeoNearExecution(operation, method, accessor, this);
		} else if (method.isGeoNearQuery()) {
			return new GeoNearExecution(operation, method, accessor);
		} else if (method.isSliceQuery()) {
			return new SlicedExecution(operation, accessor.getPageable());
		} else if (method.isStreamQuery()) {
			return q -> operation.matching(q).stream();
		} else if (method.isCollectionQuery()) {
			return q -> operation.matching(q.with(accessor.getPageable())).all();
		} else if (method.isPageQuery()) {
			return new PagedExecution(operation, accessor.getPageable());
		} else if (isCountQuery()) {
			return q -> operation.matching(q).count();
		} else if (isExistsQuery()) {
			return q -> operation.matching(q).exists();
		} else {
			return q -> operation.matching(q).oneValue();
		}
	}

	Query applyQueryMetaAttributesWhenPresent(Query query) {

		if (method.hasQueryMetaAttributes()) {
			query.setMeta(method.getQueryMetaAttributes());
		}

		return query;
	}

	/**
	 * Creates a {@link Query} instance using the given {@link ConvertingParameterAccessor}. Will delegate to
	 * {@link #createQuery(ConvertingParameterAccessor)} by default but allows customization of the count query to be
	 * triggered.
	 *
	 * @param accessor must not be {@literal null}.
	 * @return
	 */
	protected Query createCountQuery(ConvertingParameterAccessor accessor) {
		return applyQueryMetaAttributesWhenPresent(createQuery(accessor));
	}

	/**
	 * Creates a {@link Query} instance using the given {@link ParameterAccessor}
	 *
	 * @param accessor must not be {@literal null}.
	 * @return
	 */
	protected abstract Query createQuery(ConvertingParameterAccessor accessor);

	/**
	 * Returns whether the query should get a count projection applied.
	 *
	 * @return
	 */
	protected abstract boolean isCountQuery();

	/**
	 * Returns whether the query should get an exists projection applied.
	 *
	 * @return
	 * @since 1.10
	 */
	protected abstract boolean isExistsQuery();

	/**
	 * Return weather the query should delete matching documents.
	 *
	 * @return
	 * @since 1.5
	 */
	protected abstract boolean isDeleteQuery();
}
