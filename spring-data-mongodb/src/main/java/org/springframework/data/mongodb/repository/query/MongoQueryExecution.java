/*
 * Copyright 2016-2025 the original author or authors.
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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

import org.jspecify.annotations.Nullable;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Range;
import org.springframework.data.domain.ScoringFunction;
import org.springframework.data.domain.SearchResult;
import org.springframework.data.domain.SearchResults;
import org.springframework.data.domain.Similarity;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoPage;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.ExecutableAggregationOperation.TerminatingAggregation;
import org.springframework.data.mongodb.core.ExecutableFindOperation;
import org.springframework.data.mongodb.core.ExecutableFindOperation.FindWithQuery;
import org.springframework.data.mongodb.core.ExecutableFindOperation.TerminatingFind;
import org.springframework.data.mongodb.core.ExecutableRemoveOperation;
import org.springframework.data.mongodb.core.ExecutableRemoveOperation.ExecutableRemove;
import org.springframework.data.mongodb.core.ExecutableRemoveOperation.TerminatingRemove;
import org.springframework.data.mongodb.core.ExecutableUpdateOperation.ExecutableUpdate;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.aggregation.AggregationPipeline;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.TypedAggregation;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.data.mongodb.repository.query.VectorSearchDelegate.QueryContainer;
import org.springframework.data.mongodb.repository.util.SliceUtils;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.support.PageableExecutionUtils;
import org.springframework.data.util.TypeInformation;
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
public interface MongoQueryExecution {

	@Nullable
	Object execute(Query query);

	/**
	 * {@link MongoQueryExecution} for {@link Slice} query methods.
	 *
	 * @author Oliver Gierke
	 * @author Christoph Strobl
	 * @since 1.5
	 */
	final class SlicedExecution<T> implements MongoQueryExecution {

		private final FindWithQuery<T> find;
		private final Pageable pageable;

		public SlicedExecution(ExecutableFindOperation.FindWithQuery<T> find, Pageable pageable) {

			Assert.notNull(find, "Find must not be null");
			Assert.notNull(pageable, "Pageable must not be null");

			this.find = find;
			this.pageable = pageable;
		}

		@Override
		@SuppressWarnings({ "unchecked", "rawtypes" })
		public Slice<T> execute(Query query) {

			int pageSize = pageable.getPageSize();

			// Apply Pageable but tweak limit to peek into next page
			Query modifiedQuery = SliceUtils.limitResult(query, pageable).with(pageable.getSort());
			List result = find.matching(modifiedQuery).all();

			boolean hasNext = result.size() > pageSize;

			return new SliceImpl<T>(hasNext ? result.subList(0, pageSize) : result, pageable, hasNext);
		}
	}

	/**
	 * {@link MongoQueryExecution} for pagination queries.
	 *
	 * @author Oliver Gierke
	 * @author Mark Paluch
	 * @author Christoph Strobl
	 */
	final class PagedExecution<T> implements MongoQueryExecution {

		private final FindWithQuery<T> operation;
		private final Pageable pageable;

		public PagedExecution(ExecutableFindOperation.FindWithQuery<T> operation, Pageable pageable) {

			Assert.notNull(operation, "Operation must not be null");
			Assert.notNull(pageable, "Pageable must not be null");

			this.operation = operation;
			this.pageable = pageable;
		}

		@Override
		public Page<T> execute(Query query) {

			int overallLimit = query.getLimit();

			TerminatingFind<T> matching = operation.matching(query);

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

		GeoResults<Object> doExecuteQuery(Query query) {
			return doExecuteQuery(nearQuery(query));
		}

		NearQuery nearQuery(Query query) {

			Point nearLocation = accessor.getGeoNearLocation();
			Assert.notNull(nearLocation, "[query.location] must not be null");

			NearQuery nearQuery = NearQuery.near(nearLocation);

			if (query != null) {
				nearQuery.query(query);
			}

			Range<Distance> distances = accessor.getDistanceRange();
			Assert.notNull(nearLocation, "[query.distance] must not be null");

			distances.getLowerBound().getValue().ifPresent(it -> nearQuery.minDistance(it).in(it.getMetric()));
			distances.getUpperBound().getValue().ifPresent(it -> nearQuery.maxDistance(it).in(it.getMetric()));

			Pageable pageable = accessor.getPageable();
			return nearQuery.with(pageable);
		}

		@SuppressWarnings({ "unchecked", "NullAway" })
		GeoResults<Object> doExecuteQuery(NearQuery query) {
			return (GeoResults<Object>) operation.near(query).all();
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
	 * {@link MongoQueryExecution} to execute vector search.
	 *
	 * @author Mark Paluch
	 * @author Chistoph Strobl
	 * @since 5.0
	 */
	class VectorSearchExecution implements MongoQueryExecution {

		private final MongoOperations operations;
		private final TypeInformation<?> returnType;
		private final String collectionName;
		private final Class<?> targetType;
		private final ScoringFunction scoringFunction;
		private final AggregationPipeline pipeline;

		VectorSearchExecution(MongoOperations operations, MongoQueryMethod method, String collectionName,
				QueryContainer queryContainer) {
			this(operations, queryContainer.outputType(), collectionName, method.getReturnType(), queryContainer.pipeline(),
					queryContainer.scoringFunction());
		}

		public VectorSearchExecution(MongoOperations operations, Class<?> targetType, String collectionName,
				TypeInformation<?> returnType, AggregationPipeline pipeline, ScoringFunction scoringFunction) {

			this.operations = operations;
			this.returnType = returnType;
			this.collectionName = collectionName;
			this.targetType = targetType;
			this.scoringFunction = scoringFunction;
			this.pipeline = pipeline;
		}

		@Override
		@SuppressWarnings({ "unchecked", "rawtypes" })
		public Object execute(Query query) {

			TerminatingAggregation<?> executableAggregation = operations.aggregateAndReturn(targetType)
					.inCollection(collectionName).by(TypedAggregation.newAggregation(targetType, pipeline.getOperations()));

			if (!isSearchResult(returnType)) {
				return executableAggregation.all().getMappedResults();
			}

			AggregationResults<? extends SearchResult<?>> result = executableAggregation
					.map((raw, container) -> new SearchResult<>(container.get(),
							Similarity.raw(raw.getDouble("__score__"), scoringFunction)))
					.all();

			return isListOfSearchResult(returnType) ? result.getMappedResults()
					: new SearchResults(result.getMappedResults());
		}

		private static boolean isListOfSearchResult(TypeInformation<?> returnType) {

			if (!Collection.class.isAssignableFrom(returnType.getType())) {
				return false;
			}

			TypeInformation<?> componentType = returnType.getComponentType();
			return componentType != null && SearchResult.class.equals(componentType.getType());
		}

		private static boolean isSearchResult(TypeInformation<?> returnType) {

			if (SearchResults.class.isAssignableFrom(returnType.getType())) {
				return true;
			}

			if (!Iterable.class.isAssignableFrom(returnType.getType())) {
				return false;
			}

			TypeInformation<?> componentType = returnType.getComponentType();
			return componentType != null && SearchResult.class.equals(componentType.getType());
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

			NearQuery nearQuery = nearQuery(query);
			GeoResults<Object> geoResults = doExecuteQuery(nearQuery);

			Page<GeoResult<Object>> page = PageableExecutionUtils.getPage(geoResults.getContent(), accessor.getPageable(),
					() -> operation.near(nearQuery).count());

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
	final class DeleteExecution<T> implements MongoQueryExecution {

		private ExecutableRemoveOperation.ExecutableRemove<T> remove;
		private Type type;

		public DeleteExecution(ExecutableRemove<T> remove, QueryMethod queryMethod) {
			this.remove = remove;
			if (queryMethod.isCollectionQuery()) {
				this.type = Type.FIND_AND_REMOVE_ALL;
			} else if (queryMethod.isQueryForEntity()
					&& !ClassUtils.isPrimitiveOrWrapper(queryMethod.getReturnedObjectType())) {
				this.type = Type.FIND_AND_REMOVE_ONE;
			} else {
				this.type = Type.ALL;
			}
		}

		public DeleteExecution(ExecutableRemove<T> remove, Type type) {
			this.remove = remove;
			this.type = type;
		}

		@Override
		public @Nullable Object execute(Query query) {

			TerminatingRemove<T> doRemove = remove.matching(query);
			if (Type.ALL.equals(type)) {
				DeleteResult result = doRemove.all();
				return result.wasAcknowledged() ? Long.valueOf(result.getDeletedCount()) : Long.valueOf(0);
			} else if (Type.FIND_AND_REMOVE_ALL.equals(type)) {
				return doRemove.findAndRemove();
			} else if (Type.FIND_AND_REMOVE_ONE.equals(type)) {
				Iterator<T> removed = doRemove.findAndRemove().iterator();
				return removed.hasNext() ? removed.next() : null;
			}
			throw new RuntimeException();
		}

		public enum Type {
			FIND_AND_REMOVE_ONE, FIND_AND_REMOVE_ALL, ALL
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
