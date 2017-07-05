/*
 * Copyright 2016-2017 the original author or authors.
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

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.List;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Range;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoPage;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.ExecutableFindOperation;
import org.springframework.data.mongodb.core.ExecutableFindOperation.FindOperationWithQuery;
import org.springframework.data.mongodb.core.ExecutableFindOperation.TerminatingFindOperation;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.repository.support.PageableExecutionUtils;
import org.springframework.data.util.CloseableIterator;
import org.springframework.data.util.StreamUtils;
import org.springframework.data.util.TypeInformation;

import com.mongodb.client.result.DeleteResult;

/**
 * Set of classes to contain query execution strategies. Depending (mostly) on the return type of a
 * {@link org.springframework.data.repository.query.QueryMethod} a {@link AbstractMongoQuery} can be executed in various
 * flavors.
 *
 * @author Oliver Gierke
 * @author Mark Paluch
 * @author Christoph Strobl
 */
interface MongoQueryExecution {

	Object execute(Query query);

	/**
	 * {@link MongoQueryExecution} for collection returning queries.
	 *
	 * @author Oliver Gierke
	 */
	@RequiredArgsConstructor
	final class CollectionExecution implements MongoQueryExecution {

		private final @NonNull ExecutableFindOperation.FindOperationWithQuery<?> find;
		private final @NonNull Pageable pageable;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.repository.query.MongoQueryExecution#execute(org.springframework.data.mongodb.core.query.Query)
		 */
		@Override
		public Object execute(Query query) {
			return find.matching(query.with(pageable)).all();
		}
	}

	/**
	 * {@link MongoQueryExecution} for {@link Slice} query methods.
	 *
	 * @author Oliver Gierke
	 * @author Christoph Strobl
	 * @since 1.5
	 */
	@RequiredArgsConstructor
	static final class SlicedExecution implements MongoQueryExecution {

		private final @NonNull FindOperationWithQuery<?> find;
		private final @NonNull Pageable pageable;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.repository.query.MongoQueryExecution#execute(org.springframework.data.mongodb.core.query.Query)
		 */
		@Override
		@SuppressWarnings({ "unchecked", "rawtypes" })
		public Object execute(Query query) {

			int pageSize = pageable.getPageSize();

			// Apply Pageable but tweak limit to peek into next page
			Query modifiedQuery = query.with(pageable).limit(pageSize + 1);
			List result = find.matching(modifiedQuery).all();

			boolean hasNext = result.size() > pageSize;

			return new SliceImpl<Object>(hasNext ? result.subList(0, pageSize) : result, pageable, hasNext);
		}
	}

	/**
	 * {@link MongoQueryExecution} for pagination queries.
	 *
	 * @author Oliver Gierke
	 * @author Mark Paluch
	 */
	@RequiredArgsConstructor
	static final class PagedExecution implements MongoQueryExecution {

		private final @NonNull FindOperationWithQuery<?> operation;
		private final @NonNull Pageable pageable;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.repository.query.MongoQueryExecution#execute(org.springframework.data.mongodb.core.query.Query)
		 */
		@Override
		public Object execute(Query query) {

			int overallLimit = query.getLimit();

			TerminatingFindOperation<?> matching = operation.matching(query);

			// Apply raw pagination
			query.with(pageable);

			// Adjust limit if page would exceed the overall limit
			if (overallLimit != 0 && pageable.getOffset() + pageable.getPageSize() > overallLimit) {
				query.limit((int) (overallLimit - pageable.getOffset()));
			}

			return PageableExecutionUtils.getPage(matching.all(), pageable, () -> {

				long count = matching.count();
				return overallLimit != 0 ? Math.min(count, overallLimit) : count;
			});
		}
	}

	/**
	 * {@link MongoQueryExecution} to perform a count projection.
	 *
	 * @author Oliver Gierke
	 * @author Mark Paluch
	 * @since 1.10
	 */
	@RequiredArgsConstructor
	static final class CountExecution implements MongoQueryExecution {

		private final @NonNull FindOperationWithQuery<?> operation;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.repository.query.MongoQueryExecution#execute(org.springframework.data.mongodb.core.query.Query)
		 */
		@Override
		public Object execute(Query query) {
			return operation.count();
		}
	}

	/**
	 * {@link MongoQueryExecution} to execute geo-near queries.
	 *
	 * @author Oliver Gierke
	 */
	@RequiredArgsConstructor
	static class GeoNearExecution implements MongoQueryExecution {

		private final MongoOperations operations;
		private final MongoQueryMethod method;
		private final MongoParameterAccessor accessor;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.repository.query.AbstractMongoQuery.Execution#execute(org.springframework.data.mongodb.core.query.Query, java.lang.Class, java.lang.String)
		 */
		@Override
		public Object execute(Query query) {

			GeoResults<?> results = doExecuteQuery(query);
			return isListOfGeoResult(method.getReturnType()) ? results.getContent() : results;
		}

		@SuppressWarnings("unchecked")
		protected GeoResults<Object> doExecuteQuery(Query query) {

			Class<?> type = method.getReturnedObjectType();
			String collection = method.getEntityInformation().getCollectionName();

			Point nearLocation = accessor.getGeoNearLocation();
			NearQuery nearQuery = NearQuery.near(nearLocation);

			if (query != null) {
				nearQuery.query(query);
			}

			Range<Distance> distances = accessor.getDistanceRange();
			distances.getLowerBound().getValue().ifPresent(it -> nearQuery.minDistance(it).in(it.getMetric()));
			distances.getUpperBound().getValue().ifPresent(it -> nearQuery.maxDistance(it).in(it.getMetric()));

			Pageable pageable = accessor.getPageable();

			if (pageable != null) {
				nearQuery.with(pageable);
			}

			return (GeoResults<Object>) operations.geoNear(nearQuery, type, collection);
		}

		private static boolean isListOfGeoResult(TypeInformation<?> returnType) {

			if (!returnType.getType().equals(List.class)) {
				return false;
			}

			TypeInformation<?> componentType = returnType.getComponentType();
			return componentType != null && GeoResult.class.equals(componentType.getType());
		}
	}

	/**
	 * {@link MongoQueryExecution} to execute geo-near queries with paging.
	 *
	 * @author Oliver Gierke
	 * @author Mark Paluch
	 */
	static final class PagingGeoNearExecution extends GeoNearExecution {

		private final MongoOperations operations;
		private final MongoQueryMethod method;
		private final MongoParameterAccessor accessor;
		private final AbstractMongoQuery mongoQuery;

		public PagingGeoNearExecution(MongoOperations operations, MongoQueryMethod method, MongoParameterAccessor accessor,
				AbstractMongoQuery query) {

			super(operations, method, accessor);

			this.accessor = accessor;
			this.operations = operations;
			this.mongoQuery = query;
			this.method = method;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.repository.query.MongoQueryExecution.GeoNearExecution#execute(org.springframework.data.mongodb.core.query.Query, java.lang.Class, java.lang.String)
		 */
		@Override
		public Object execute(Query query) {

			String collectionName = method.getEntityInformation().getCollectionName();

			GeoResults<Object> geoResults = doExecuteQuery(query);

			Page<GeoResult<Object>> page = PageableExecutionUtils.getPage(geoResults.getContent(), accessor.getPageable(),
					() -> {

						Query countQuery = mongoQuery
								.createCountQuery(new ConvertingParameterAccessor(operations.getConverter(), accessor));
						countQuery = mongoQuery.applyQueryMetaAttributesWhenPresent(countQuery);

						return operations.count(countQuery, collectionName);

					});

			// transform to GeoPage after applying optimization
			return new GeoPage<Object>(geoResults, accessor.getPageable(), page.getTotalElements());
		}
	}

	/**
	 * {@link MongoQueryExecution} removing documents matching the query.
	 *
	 * @since 1.5
	 */
	@RequiredArgsConstructor
	static final class DeleteExecution implements MongoQueryExecution {

		private final MongoOperations operations;
		private final MongoQueryMethod method;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.repository.query.MongoQueryExecution#execute(org.springframework.data.mongodb.core.query.Query)
		 */
		@Override
		public Object execute(Query query) {

			String collectionName = method.getEntityInformation().getCollectionName();
			Class<?> type = method.getEntityInformation().getJavaType();

			if (method.isCollectionQuery()) {
				return operations.findAllAndRemove(query, type, collectionName);
			}

			DeleteResult writeResult = operations.remove(query, type, collectionName);
			return writeResult != null ? writeResult.getDeletedCount() : 0L;
		}
	}

	/**
	 * @author Thomas Darimont
	 * @since 1.7
	 */
	@RequiredArgsConstructor
	static final class StreamExecution implements MongoQueryExecution {

		private final @NonNull FindOperationWithQuery<?> operation;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.repository.query.MongoQueryExecution#execute(org.springframework.data.mongodb.core.query.Query)
		 */
		@Override
		@SuppressWarnings("unchecked")
		public Object execute(Query query) {

			TerminatingFindOperation<?> matching = operation.matching(query);

			return StreamUtils.createStreamFromIterator((CloseableIterator<Object>) matching.stream());
		}
	}
}
