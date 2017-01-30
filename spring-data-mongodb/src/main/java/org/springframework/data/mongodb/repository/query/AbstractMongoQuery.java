/*
 * Copyright 2010-2016 the original author or authors.
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

import java.util.Optional;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.EntityInstantiators;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.query.MongoQueryExecution.CollectionExecution;
import org.springframework.data.mongodb.repository.query.MongoQueryExecution.CountExecution;
import org.springframework.data.mongodb.repository.query.MongoQueryExecution.DeleteExecution;
import org.springframework.data.mongodb.repository.query.MongoQueryExecution.ExistsExecution;
import org.springframework.data.mongodb.repository.query.MongoQueryExecution.GeoNearExecution;
import org.springframework.data.mongodb.repository.query.MongoQueryExecution.PagedExecution;
import org.springframework.data.mongodb.repository.query.MongoQueryExecution.PagingGeoNearExecution;
import org.springframework.data.mongodb.repository.query.MongoQueryExecution.ResultProcessingConverter;
import org.springframework.data.mongodb.repository.query.MongoQueryExecution.ResultProcessingExecution;
import org.springframework.data.mongodb.repository.query.MongoQueryExecution.SingleEntityExecution;
import org.springframework.data.mongodb.repository.query.MongoQueryExecution.SlicedExecution;
import org.springframework.data.mongodb.repository.query.MongoQueryExecution.StreamExecution;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.ResultProcessor;
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
	private final EntityInstantiators instantiators;

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
		this.instantiators = new EntityInstantiators();
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

		MongoParameterAccessor accessor = new MongoParametersParameterAccessor(method, parameters);
		Query query = createQuery(new ConvertingParameterAccessor(operations.getConverter(), accessor));

		applyQueryMetaAttributesWhenPresent(query);

		ResultProcessor processor = method.getResultProcessor().withDynamicProjection(Optional.of(accessor));
		String collection = method.getEntityInformation().getCollectionName();

		MongoQueryExecution execution = getExecution(query, accessor,
				new ResultProcessingConverter(processor, operations, instantiators));

		return execution.execute(query, processor.getReturnedType().getDomainType(), collection);
	}

	/**
	 * Returns the execution instance to use.
	 * 
	 * @param query must not be {@literal null}.
	 * @param parameters must not be {@literal null}.
	 * @param accessor must not be {@literal null}.
	 * @return
	 */
	private MongoQueryExecution getExecution(Query query, MongoParameterAccessor accessor,
			Converter<Object, Object> resultProcessing) {

		if (method.isStreamQuery()) {
			return new StreamExecution(operations, resultProcessing);
		}

		return new ResultProcessingExecution(getExecutionToWrap(query, accessor), resultProcessing);
	}

	private MongoQueryExecution getExecutionToWrap(Query query, MongoParameterAccessor accessor) {

		if (isDeleteQuery()) {
			return new DeleteExecution(operations, method);
		} else if (method.isGeoNearQuery() && method.isPageQuery()) {
			return new PagingGeoNearExecution(operations, accessor, method.getReturnType(), this);
		} else if (method.isGeoNearQuery()) {
			return new GeoNearExecution(operations, accessor, method.getReturnType());
		} else if (method.isSliceQuery()) {
			return new SlicedExecution(operations, accessor.getPageable());
		} else if (method.isCollectionQuery()) {
			return new CollectionExecution(operations, accessor.getPageable());
		} else if (method.isPageQuery()) {
			return new PagedExecution(operations, accessor.getPageable());
		} else if (isCountQuery()) {
			return new CountExecution(operations);
		} else if (isExistsQuery()) {
			return new ExistsExecution(operations);
		} else {
			return new SingleEntityExecution(operations);
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
