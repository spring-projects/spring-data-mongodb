/*
 * Copyright 2010-2014 the original author or authors.
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

import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;

import com.mongodb.WriteResult;

/**
 * Base class for {@link RepositoryQuery} implementations for Mongo.
 * 
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 */
public abstract class AbstractMongoQuery implements RepositoryQuery {

	private static final ConversionService CONVERSION_SERVICE = new DefaultConversionService();

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

		Object result = null;

		if (isDeleteQuery()) {
			result = new DeleteExecution().execute(query);
		} else if (method.isGeoNearQuery() && method.isPageQuery()) {

			MongoParameterAccessor countAccessor = new MongoParametersParameterAccessor(method, parameters);
			Query countQuery = createCountQuery(new ConvertingParameterAccessor(operations.getConverter(), countAccessor));

			result = new GeoNearExecution(accessor).execute(query, countQuery);
		} else if (method.isGeoNearQuery()) {
			result = new GeoNearExecution(accessor).execute(query);
		} else if (method.isSliceQuery()) {
			result = new SlicedExecution(accessor.getPageable()).execute(query);
		} else if (method.isCollectionQuery()) {
			result = new CollectionExecution(accessor.getPageable()).execute(query);
		} else if (method.isPageQuery()) {
			result = new PagedExecution(accessor.getPageable()).execute(query);
		} else {
			result = new SingleEntityExecution(isCountQuery()).execute(query);
		}

		if (result == null) {
			return result;
		}

		Class<?> expectedReturnType = method.getReturnType().getType();

		if (expectedReturnType.isAssignableFrom(result.getClass())) {
			return result;
		}

		return CONVERSION_SERVICE.convert(result, expectedReturnType);
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
		return createQuery(accessor);
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
	 * Return weather the query should delete matching documents.
	 * 
	 * @return
	 * @since 1.5
	 */
	protected abstract boolean isDeleteQuery();

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
	final class CollectionExecution extends Execution {

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
	 * {@link Execution} for {@link Slice} query methods.
	 * 
	 * @author Oliver Gierke
	 * @since 1.5
	 */

	final class SlicedExecution extends Execution {

		private final Pageable pageable;

		SlicedExecution(Pageable pageable) {
			this.pageable = pageable;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.repository.query.AbstractMongoQuery.Execution#execute(org.springframework.data.mongodb.core.query.Query)
		 */
		@Override
		@SuppressWarnings({ "unchecked", "rawtypes" })
		Object execute(Query query) {

			MongoEntityMetadata<?> metadata = method.getEntityInformation();
			int pageSize = pageable.getPageSize();
			Pageable slicePageable = new PageRequest(pageable.getPageNumber(), pageSize + 1, pageable.getSort());

			List result = operations.find(query.with(slicePageable), metadata.getJavaType(), metadata.getCollectionName());

			boolean hasNext = result.size() > pageSize;

			return new SliceImpl<Object>(hasNext ? result.subList(0, pageSize) : result, pageable, hasNext);
		}
	}

	/**
	 * {@link Execution} for pagination queries.
	 * 
	 * @author Oliver Gierke
	 */
	final class PagedExecution extends Execution {

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
	final class SingleEntityExecution extends Execution {

		private final boolean countProjection;

		private SingleEntityExecution(boolean countProjection) {
			this.countProjection = countProjection;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.repository.AbstractMongoQuery.Execution#execute(org.springframework.data.mongodb.core.core.query.Query)
		 */
		@Override
		Object execute(Query query) {

			MongoEntityMetadata<?> metadata = method.getEntityInformation();
			return countProjection ? operations.count(query, metadata.getJavaType()) : operations.findOne(query,
					metadata.getJavaType(), metadata.getCollectionName());
		}
	}

	/**
	 * {@link Execution} to execute geo-near queries.
	 * 
	 * @author Oliver Gierke
	 */
	@SuppressWarnings("deprecation")
	final class GeoNearExecution extends Execution {

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

			return new org.springframework.data.mongodb.core.geo.GeoPage<Object>(doExecuteQuery(query),
					accessor.getPageable(), count);
		}

		@SuppressWarnings("unchecked")
		private org.springframework.data.mongodb.core.geo.GeoResults<Object> doExecuteQuery(Query query) {

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
			return (org.springframework.data.mongodb.core.geo.GeoResults<Object>) operations.geoNear(nearQuery,
					metadata.getJavaType(), metadata.getCollectionName());
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

	/**
	 * {@link Execution} removing documents matching the query.
	 * 
	 * @since 1.5
	 */
	final class DeleteExecution extends Execution {

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.repository.query.AbstractMongoQuery.Execution#execute(org.springframework.data.mongodb.core.query.Query)
		 */
		@Override
		Object execute(Query query) {

			MongoEntityMetadata<?> metadata = method.getEntityInformation();
			return deleteAndConvertResult(query, metadata);
		}

		private Object deleteAndConvertResult(Query query, MongoEntityMetadata<?> metadata) {

			if (method.isCollectionQuery()) {
				return operations.findAllAndRemove(query, metadata.getJavaType(), metadata.getCollectionName());
			}

			WriteResult writeResult = operations.remove(query, metadata.getJavaType(), metadata.getCollectionName());
			return writeResult != null ? writeResult.getN() : 0L;
		}
	}
}
