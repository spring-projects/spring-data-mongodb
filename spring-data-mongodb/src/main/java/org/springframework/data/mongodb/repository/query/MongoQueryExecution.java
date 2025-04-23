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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

import org.bson.Document;
import org.jspecify.annotations.Nullable;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Range;
import org.springframework.data.domain.Score;
import org.springframework.data.domain.SearchResult;
import org.springframework.data.domain.SearchResults;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Vector;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoPage;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.ExecutableFindOperation;
import org.springframework.data.mongodb.core.ExecutableFindOperation.FindWithQuery;
import org.springframework.data.mongodb.core.ExecutableFindOperation.TerminatingFind;
import org.springframework.data.mongodb.core.ExecutableRemoveOperation;
import org.springframework.data.mongodb.core.ExecutableRemoveOperation.ExecutableRemove;
import org.springframework.data.mongodb.core.ExecutableRemoveOperation.TerminatingRemove;
import org.springframework.data.mongodb.core.ExecutableUpdateOperation.ExecutableUpdate;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.TypedAggregation;
import org.springframework.data.mongodb.core.aggregation.VectorSearchOperation;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
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

		@SuppressWarnings({"unchecked","NullAway"})
		GeoResults<Object> doExecuteQuery(Query query) {

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
	 * {@link MongoQueryExecution} to execute vector search
	 *
	 * @author Mark Paluch
	 * @since 5.0
	 */
	class VectorSearchExecution implements MongoQueryExecution {

		private final MongoOperations operations;
		private final MongoQueryMethod method;
		private final String collectionName;
		private final @Nullable Integer numCandidates;
		private final VectorSearchOperation.SearchType searchType;
		private final MongoParameterAccessor accessor;
		private final Class<Object> outputType;
		private final String path;

		public VectorSearchExecution(MongoOperations operations, MongoQueryMethod method, String collectionName,
				String path, @Nullable Integer numCandidates, VectorSearchOperation.SearchType searchType,
				MongoParameterAccessor accessor, Class<Object> outputType) {

			this.operations = operations;
			this.collectionName = collectionName;
			this.path = path;
			this.numCandidates = numCandidates;
			this.method = method;
			this.searchType = searchType;
			this.accessor = accessor;
			this.outputType = outputType;
		}

		@Override
		public Object execute(Query query) {

			SearchResults<?> results = doExecuteQuery(query);
			return isListOfSearchResult(method.getReturnType()) ? results.getContent() : results;
		}

		@SuppressWarnings("unchecked")
		SearchResults<Object> doExecuteQuery(Query query) {

			Vector vector = accessor.getVector();
			Score score = accessor.getScore();
			Range<Score> distance = accessor.getScoreRange();
			int limit;

			if (query.isLimited()) {
				limit = query.getLimit();
			} else {
				limit = Math.max(1, numCandidates != null ? numCandidates / 20 : 1);
			}

			List<AggregationOperation> stages = new ArrayList<>();
			VectorSearchOperation $vectorSearch = Aggregation.vectorSearch(method.getAnnotatedHint()).path(path)
					.vector(vector).limit(limit);

			if (numCandidates != null) {
				$vectorSearch = $vectorSearch.numCandidates(numCandidates);
			}

			$vectorSearch = $vectorSearch.filter(query.getQueryObject());
			$vectorSearch = $vectorSearch.searchType(searchType);
			$vectorSearch = $vectorSearch.withSearchScore("__score__");

			if (score != null) {
				$vectorSearch = $vectorSearch.withFilterBySore(c -> {
					c.gt(score.getValue());
				});
			} else if (distance.getLowerBound().isBounded() || distance.getUpperBound().isBounded()) {
				$vectorSearch = $vectorSearch.withFilterBySore(c -> {
					Range.Bound<Score> lower = distance.getLowerBound();
					if (lower.isBounded()) {
						double value = lower.getValue().get().getValue();
						if (lower.isInclusive()) {
							c.gte(value);
						} else {
							c.gt(value);
						}
					}

					Range.Bound<Score> upper = distance.getUpperBound();
					if (upper.isBounded()) {

						double value = upper.getValue().get().getValue();
						if (upper.isInclusive()) {
							c.lte(value);
						} else {
							c.lt(value);
						}
					}
				});
			}

			stages.add($vectorSearch);

			if (query.isSorted()) {
				// TODO stages.add(Aggregation.sort(query.with()));
			} else {
				stages.add(Aggregation.sort(Sort.Direction.DESC, "__score__"));
			}

			AggregationResults<Object> aggregated = operations
					.aggregate(TypedAggregation.<Object> newAggregation(outputType, stages), collectionName, outputType);

			List<Object> mappedResults = aggregated.getMappedResults();
			List<org.bson.Document> rawResults = aggregated.getRawResults().getList("results", org.bson.Document.class);

			List<SearchResult<Object>> result = new ArrayList<>(mappedResults.size());

			for (int i = 0; i < mappedResults.size(); i++) {
				Document document = rawResults.get(i);
				SearchResult<Object> searchResult = new SearchResult<>(mappedResults.get(i),
						Score.of(document.getDouble("__score__")));

				result.add(searchResult);
			}

			return new SearchResults<>(result);
		}

		private static boolean isListOfSearchResult(TypeInformation<?> returnType) {

			if (!returnType.getType().equals(List.class)) {
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
