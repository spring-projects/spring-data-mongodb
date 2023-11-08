/*
 * Copyright 2016-2023 the original author or authors.
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

import java.util.List;
import java.util.function.Supplier;

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
import org.springframework.data.mongodb.core.ExecutableFindOperation.FindWithQuery;
import org.springframework.data.mongodb.core.ExecutableFindOperation.TerminatingFind;
import org.springframework.data.mongodb.core.ExecutableUpdateOperation.ExecutableUpdate;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.data.support.PageableExecutionUtils;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

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
@FunctionalInterface
interface MongoQueryExecution {

	@Nullable
	Object execute(Query query);

	/**
	 * {@link MongoQueryExecution} for {@link Slice} query methods.
	 *
	 * @author Oliver Gierke
	 * @author Christoph Strobl
	 * @since 1.5
	 */
	final class SlicedExecution implements MongoQueryExecution {

		private final FindWithQuery<?> find;
		private final Pageable pageable;

		public SlicedExecution(ExecutableFindOperation.FindWithQuery<?> find, Pageable pageable) {

			Assert.notNull(find, "Find must not be null");
			Assert.notNull(pageable, "Pageable must not be null");

			this.find = find;
			this.pageable = pageable;
		}

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
	 * @author Christoph Strobl
	 */
	final class PagedExecution implements MongoQueryExecution {

		private final FindWithQuery<?> operation;
		private final Pageable pageable;

		public PagedExecution(ExecutableFindOperation.FindWithQuery<?> operation, Pageable pageable) {

			Assert.notNull(operation, "Operation must not be null");
			Assert.notNull(pageable, "Pageable must not be null");

			this.operation = operation;
			this.pageable = pageable;
		}

		@Override
		public Object execute(Query query) {

			int overallLimit = query.getLimit();

			TerminatingFind<?> matching = operation.matching(query);

			// Apply raw pagination
			query.with(pageable);

			// Adjust limit if page would exceed the overall limit
			if (overallLimit != 0 && pageable.getOffset() + pageable.getPageSize() > overallLimit) {
				query.limit((int) (overallLimit - pageable.getOffset()));
			}

			return PageableExecutionUtils.getPage(matching.all(), pageable, () -> {

				long count = operation.matching(Query.of(query).skip(-1).limit(-1)).count();
				return overallLimit != 0 ? Math.min(count, overallLimit) : count;
			});
		}
	}

	/**
	 * {@link MongoQueryExecution} to execute geo-near queries.
	 *
	 * @author Oliver Gierke
	 */
	class GeoNearExecution implements MongoQueryExecution {

		private final FindWithQuery<?> operation;
		private final MongoQueryMethod method;
		private final MongoParameterAccessor accessor;

		public GeoNearExecution(ExecutableFindOperation.FindWithQuery<?> operation, MongoQueryMethod method,
				MongoParameterAccessor accessor) {

			Assert.notNull(operation, "Operation must not be null");
			Assert.notNull(method, "Method must not be null");
			Assert.notNull(accessor, "Accessor must not be null");

			this.operation = operation;
			this.method = method;
			this.accessor = accessor;
		}

		@Override
		public Object execute(Query query) {

			GeoResults<?> results = doExecuteQuery(query);
			return isListOfGeoResult(method.getReturnType()) ? results.getContent() : results;
		}

		@SuppressWarnings("unchecked")
		GeoResults<Object> doExecuteQuery(Query query) {

			Point nearLocation = accessor.getGeoNearLocation();
			NearQuery nearQuery = NearQuery.near(nearLocation);

			if (query != null) {
				nearQuery.query(query);
			}

			Range<Distance> distances = accessor.getDistanceRange();
			distances.getLowerBound().getValue().ifPresent(it -> nearQuery.minDistance(it).in(it.getMetric()));
			distances.getUpperBound().getValue().ifPresent(it -> nearQuery.maxDistance(it).in(it.getMetric()));

			Pageable pageable = accessor.getPageable();
			nearQuery.with(pageable);

			return (GeoResults<Object>) operation.near(nearQuery).all();
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
	final class PagingGeoNearExecution extends GeoNearExecution {

		private final FindWithQuery<?> operation;
		private final ConvertingParameterAccessor accessor;
		private final AbstractMongoQuery mongoQuery;

		PagingGeoNearExecution(FindWithQuery<?> operation, MongoQueryMethod method, ConvertingParameterAccessor accessor,
				AbstractMongoQuery query) {

			super(operation, method, accessor);

			this.accessor = accessor;
			this.operation = operation;
			this.mongoQuery = query;
		}

		@Override
		public Object execute(Query query) {

			GeoResults<Object> geoResults = doExecuteQuery(query);

			Page<GeoResult<Object>> page = PageableExecutionUtils.getPage(geoResults.getContent(), accessor.getPageable(),
					() -> {

						Query countQuery = mongoQuery.createCountQuery(accessor);
						countQuery = mongoQuery.applyQueryMetaAttributesWhenPresent(countQuery);

						return operation.matching(countQuery).count();
					});

			// transform to GeoPage after applying optimization
			return new GeoPage<>(geoResults, accessor.getPageable(), page.getTotalElements());
		}
	}

	/**
	 * {@link MongoQueryExecution} removing documents matching the query.
	 *
	 * @author Oliver Gierke
	 * @author Mark Paluch
	 * @author Artyom Gabeev
	 * @author Christoph Strobl
	 * @since 1.5
	 */
	final class DeleteExecution implements MongoQueryExecution {

		private final MongoOperations operations;
		private final MongoQueryMethod method;

		public DeleteExecution(MongoOperations operations, MongoQueryMethod method) {

			Assert.notNull(operations, "Operations must not be null");
			Assert.notNull(method, "Method must not be null");

			this.operations = operations;
			this.method = method;
		}

		@Override
		public Object execute(Query query) {

			String collectionName = method.getEntityInformation().getCollectionName();
			Class<?> type = method.getEntityInformation().getJavaType();

			if (method.isCollectionQuery()) {
				return operations.findAllAndRemove(query, type, collectionName);
			}

			if (method.isQueryForEntity() && !ClassUtils.isPrimitiveOrWrapper(method.getReturnedObjectType())) {
				return operations.findAndRemove(query, type, collectionName);
			}

			DeleteResult writeResult = operations.remove(query, type, collectionName);
			return writeResult.wasAcknowledged() ? writeResult.getDeletedCount() : 0L;
		}
	}

	/**
	 * {@link MongoQueryExecution} updating documents matching the query.
	 *
	 * @author Christph Strobl
	 * @since 3.4
	 */
	final class UpdateExecution implements MongoQueryExecution {

		private final ExecutableUpdate<?> updateOps;
		private Supplier<UpdateDefinition> updateDefinitionSupplier;
		private final MongoParameterAccessor accessor;

		UpdateExecution(ExecutableUpdate<?> updateOps, MongoQueryMethod method, Supplier<UpdateDefinition> updateSupplier,
				MongoParameterAccessor accessor) {

			this.updateOps = updateOps;
			this.updateDefinitionSupplier = updateSupplier;
			this.accessor = accessor;
		}

		@Override
		public Object execute(Query query) {

			return updateOps.matching(query.with(accessor.getSort())) //
					.apply(updateDefinitionSupplier.get()) //
					.all().getModifiedCount();
		}
	}
}
