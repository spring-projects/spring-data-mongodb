/*
 * Copyright 2010-2013 the original author or authors.
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

import java.util.List;

import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.geo.Distance;
import org.springframework.data.mongodb.core.geo.GeoPage;
import org.springframework.data.mongodb.core.geo.GeoResult;
import org.springframework.data.mongodb.core.geo.GeoResults;
import org.springframework.data.mongodb.core.geo.Point;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;

/**
 * Base class for {@link RepositoryQuery} implementations for Mongo.
 * 
 * @author Oliver Gierke
 * @author Thomas Darimont
 */
public abstract class AbstractMongoQuery implements RepositoryQuery {

	private final MongoQueryMethod method;
	private final MongoOperations operations;

	/**
	 * Creates a new {@link AbstractMongoQuery} from the given {@link MongoQueryMethod} and {@link MongoOperations}.
	 * 
	 * @param method must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 */
	public AbstractMongoQuery(MongoQueryMethod method, MongoOperations operations) {

		Assert.notNull(operations);
		Assert.notNull(method);

		this.method = method;
		this.operations = operations;
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

		if (method.isGeoNearQuery() && method.isPageQuery()) {

			MongoParameterAccessor countAccessor = new MongoParametersParameterAccessor(method, parameters);
			Query countQuery = createCountQuery(new ConvertingParameterAccessor(operations.getConverter(), countAccessor));

			return new GeoNearExecution(accessor).execute(query, countQuery);
		} else if (method.isGeoNearQuery()) {
			return new GeoNearExecution(accessor).execute(query);
		} else if (method.isCollectionQuery()) {
			return new CollectionExecution(accessor.getPageable()).execute(query);
		} else if (method.isPageQuery()) {
			return new PagedExecution(accessor.getPageable()).execute(query);
		} else {
			return new SingleEntityExecution().execute(query);
		}
	}

	/**
	 * Creates a {@link Query} instance using the given {@link ParameterAccessor}
	 * 
	 * @param accessor must not be {@literal null}.
	 * @return
	 */
	protected abstract Query createQuery(ConvertingParameterAccessor accessor);

	/**
	 * Creates a {@link Query} instance using the given {@link ConvertingParameterAccessor}. Will delegate to
	 * {@link #createQuery(ConvertingParameterAccessor)} by default but allows customization of the count query to be
	 * triggered.
	 * 
	 * @param accessor must not be {@literal null}.
	 * @return
	 */
	protected Query createCountQuery(ConvertingParameterAccessor accessor) {
		return createQuery(accessor);
	}

	private abstract class Execution {

		abstract Object execute(Query query);

		protected List<?> readCollection(Query query) {

			MongoEntityMetadata<?> metadata = method.getEntityInformation();

			String collectionName = metadata.getCollectionName();
			return operations.find(query, metadata.getJavaType(), collectionName);
		}
	}

	/**
	 * {@link Execution} for collection returning queries.
	 * 
	 * @author Oliver Gierke
	 */
	class CollectionExecution extends Execution {

		private final Pageable pageable;

		CollectionExecution(Pageable pageable) {
			this.pageable = pageable;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.repository.query.AbstractMongoQuery.Execution#execute(org.springframework.data.mongodb.core.query.Query)
		 */
		@Override
		public Object execute(Query query) {
			return readCollection(query.with(pageable));
		}
	}

	/**
	 * {@link Execution} for pagination queries.
	 * 
	 * @author Oliver Gierke
	 */
	class PagedExecution extends Execution {

		private final Pageable pageable;

		/**
		 * Creates a new {@link PagedExecution}.
		 * 
		 * @param pageable
		 */
		public PagedExecution(Pageable pageable) {

			Assert.notNull(pageable);
			this.pageable = pageable;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.repository.AbstractMongoQuery.Execution#execute(org.springframework.data.mongodb.core.query.Query)
		 */
		@Override
		@SuppressWarnings({ "rawtypes", "unchecked" })
		Object execute(Query query) {

			MongoEntityMetadata<?> metadata = method.getEntityInformation();
			long count = operations.count(query, metadata.getCollectionName());

			List<?> result = operations.find(query.with(pageable), metadata.getJavaType(), metadata.getCollectionName());

			return new PageImpl(result, pageable, count);
		}
	}

	/**
	 * {@link Execution} to return a single entity.
	 * 
	 * @author Oliver Gierke
	 */
	class SingleEntityExecution extends Execution {

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.repository.AbstractMongoQuery.Execution#execute(org.springframework.data.mongodb.core.core.query.Query)
		 */
		@Override
		Object execute(Query query) {

			MongoEntityMetadata<?> metadata = method.getEntityInformation();
			return operations.findOne(query, metadata.getJavaType());
		}
	}

	/**
	 * {@link Execution} to execute geo-near queries.
	 * 
	 * @author Oliver Gierke
	 */
	class GeoNearExecution extends Execution {

		private final MongoParameterAccessor accessor;

		public GeoNearExecution(MongoParameterAccessor accessor) {
			this.accessor = accessor;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.repository.AbstractMongoQuery.Execution#execute(org.springframework.data.mongodb.core.query.Query)
		 */
		@Override
		Object execute(Query query) {

			GeoResults<?> results = doExecuteQuery(query);
			return isListOfGeoResult() ? results.getContent() : results;
		}

		/**
		 * Executes the given {@link Query} to return a page.
		 * 
		 * @param query must not be {@literal null}.
		 * @param countQuery must not be {@literal null}.
		 * @return
		 */
		Object execute(Query query, Query countQuery) {

			MongoEntityMetadata<?> metadata = method.getEntityInformation();
			long count = operations.count(countQuery, metadata.getCollectionName());

			return new GeoPage<Object>(doExecuteQuery(query), accessor.getPageable(), count);
		}

		@SuppressWarnings("unchecked")
		private GeoResults<Object> doExecuteQuery(Query query) {

			Point nearLocation = accessor.getGeoNearLocation();
			NearQuery nearQuery = NearQuery.near(nearLocation);

			if (query != null) {
				nearQuery.query(query);
			}

			Distance maxDistance = accessor.getMaxDistance();
			if (maxDistance != null) {
				nearQuery.maxDistance(maxDistance).in(maxDistance.getMetric());
			}

			Pageable pageable = accessor.getPageable();
			if (pageable != null) {
				nearQuery.with(pageable);
			}

			MongoEntityMetadata<?> metadata = method.getEntityInformation();
			return (GeoResults<Object>) operations.geoNear(nearQuery, metadata.getJavaType(), metadata.getCollectionName());
		}

		private boolean isListOfGeoResult() {

			TypeInformation<?> returnType = method.getReturnType();

			if (!returnType.getType().equals(List.class)) {
				return false;
			}

			TypeInformation<?> componentType = returnType.getComponentType();
			return componentType == null ? false : GeoResult.class.equals(componentType.getType());
		}
	}
}
