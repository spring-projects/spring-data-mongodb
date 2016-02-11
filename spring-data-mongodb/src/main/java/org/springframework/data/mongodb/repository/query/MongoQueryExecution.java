/*
 * Copyright 2016 the original author or authors.
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

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.EntityInstantiators;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Range;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoPage;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.data.util.CloseableIterator;
import org.springframework.data.util.StreamUtils;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.ClassUtils;

import com.mongodb.WriteResult;

interface MongoQueryExecution {

	Object execute(Query query, Class<?> type, String collection);

	/**
	 * {@link MongoQueryExecution} for collection returning queries.
	 * 
	 * @author Oliver Gierke
	 */
	@RequiredArgsConstructor
	static final class CollectionExecution implements MongoQueryExecution {

		private final @NonNull MongoOperations operations;
		private final Pageable pageable;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.repository.query.AbstractMongoQuery.Execution#execute(org.springframework.data.mongodb.core.query.Query, java.lang.Class, java.lang.String)
		 */
		@Override
		public Object execute(Query query, Class<?> type, String collection) {
			return operations.find(query.with(pageable), type, collection);
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

		private final @NonNull MongoOperations operations;
		private final @NonNull Pageable pageable;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.repository.query.AbstractMongoQuery.Execution#execute(org.springframework.data.mongodb.core.query.Query, java.lang.Class, java.lang.String)
		 */
		@Override
		@SuppressWarnings({ "unchecked", "rawtypes" })
		public Object execute(Query query, Class<?> type, String collection) {

			int pageSize = pageable.getPageSize();

			// Apply Pageable but tweak limit to peek into next page
			Query modifiedQuery = query.with(pageable).limit(pageSize + 1);
			List result = operations.find(modifiedQuery, type, collection);

			boolean hasNext = result.size() > pageSize;

			return new SliceImpl<Object>(hasNext ? result.subList(0, pageSize) : result, pageable, hasNext);
		}
	}

	/**
	 * {@link MongoQueryExecution} for pagination queries.
	 * 
	 * @author Oliver Gierke
	 */
	@RequiredArgsConstructor
	static final class PagedExecution implements MongoQueryExecution {

		private final @NonNull MongoOperations operations;
		private final @NonNull Pageable pageable;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.repository.query.AbstractMongoQuery.Execution#execute(org.springframework.data.mongodb.core.query.Query, java.lang.Class, java.lang.String)
		 */
		@Override
		@SuppressWarnings({ "rawtypes", "unchecked" })
		public Object execute(Query query, Class<?> type, String collection) {

			int overallLimit = query.getLimit();
			long count = operations.count(query, type, collection);
			count = overallLimit != 0 ? Math.min(count, query.getLimit()) : count;

			boolean pageableOutOfScope = pageable.getOffset() > count;

			if (pageableOutOfScope) {
				return new PageImpl<Object>(Collections.emptyList(), pageable, count);
			}

			// Apply raw pagination
			query = query.with(pageable);

			// Adjust limit if page would exceed the overall limit
			if (overallLimit != 0 && pageable.getOffset() + pageable.getPageSize() > overallLimit) {
				query.limit(overallLimit - pageable.getOffset());
			}

			List<?> result = operations.find(query, type, collection);
			return new PageImpl(result, pageable, count);
		}
	}

	/**
	 * {@link MongoQueryExecution} to return a single entity.
	 * 
	 * @author Oliver Gierke
	 */
	@RequiredArgsConstructor
	static final class SingleEntityExecution implements MongoQueryExecution {

		private final MongoOperations operations;
		private final boolean countProjection;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.repository.query.AbstractMongoQuery.Execution#execute(org.springframework.data.mongodb.core.query.Query, java.lang.Class, java.lang.String)
		 */
		@Override
		public Object execute(Query query, Class<?> type, String collection) {
			return countProjection ? operations.count(query, type, collection) : operations.findOne(query, type, collection);
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
		private final MongoParameterAccessor accessor;
		private final TypeInformation<?> returnType;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.repository.query.AbstractMongoQuery.Execution#execute(org.springframework.data.mongodb.core.query.Query, java.lang.Class, java.lang.String)
		 */
		@Override
		public Object execute(Query query, Class<?> type, String collection) {

			GeoResults<?> results = doExecuteQuery(query, type, collection);
			return isListOfGeoResult() ? results.getContent() : results;
		}

		@SuppressWarnings("unchecked")
		protected GeoResults<Object> doExecuteQuery(Query query, Class<?> type, String collection) {

			Point nearLocation = accessor.getGeoNearLocation();
			NearQuery nearQuery = NearQuery.near(nearLocation);

			if (query != null) {
				nearQuery.query(query);
			}

			Range<Distance> distances = accessor.getDistanceRange();
			Distance maxDistance = distances.getUpperBound();

			if (maxDistance != null) {
				nearQuery.maxDistance(maxDistance).in(maxDistance.getMetric());
			}

			Distance minDistance = distances.getLowerBound();

			if (minDistance != null) {
				nearQuery.minDistance(minDistance).in(minDistance.getMetric());
			}

			Pageable pageable = accessor.getPageable();

			if (pageable != null) {
				nearQuery.with(pageable);
			}

			return (GeoResults<Object>) operations.geoNear(nearQuery, type, collection);
		}

		private boolean isListOfGeoResult() {

			if (!returnType.getType().equals(List.class)) {
				return false;
			}

			TypeInformation<?> componentType = returnType.getComponentType();
			return componentType == null ? false : GeoResult.class.equals(componentType.getType());
		}
	}

	static final class PagingGeoNearExecution extends GeoNearExecution {

		private final MongoOperations operations;
		private final MongoParameterAccessor accessor;
		private final AbstractMongoQuery mongoQuery;

		public PagingGeoNearExecution(MongoOperations operations, MongoParameterAccessor accessor,
				TypeInformation<?> returnType, AbstractMongoQuery query) {

			super(operations, accessor, returnType);

			this.accessor = accessor;
			this.operations = operations;
			this.mongoQuery = query;
		}

		/**
		 * Executes the given {@link Query} to return a page.
		 * 
		 * @param query must not be {@literal null}.
		 * @param countQuery must not be {@literal null}.
		 * @return
		 */
		@Override
		public Object execute(Query query, Class<?> type, String collection) {

			ConvertingParameterAccessor parameterAccessor = new ConvertingParameterAccessor(operations.getConverter(),
					accessor);
			Query countQuery = mongoQuery.applyQueryMetaAttributesWhenPresent(mongoQuery.createCountQuery(parameterAccessor));
			long count = operations.count(countQuery, collection);

			return new GeoPage<Object>(doExecuteQuery(query, type, collection), accessor.getPageable(), count);
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
		 * @see org.springframework.data.mongodb.repository.query.AbstractMongoQuery.Execution#execute(org.springframework.data.mongodb.core.query.Query, java.lang.Class, java.lang.String)
		 */
		@Override
		public Object execute(Query query, Class<?> type, String collection) {

			if (method.isCollectionQuery()) {
				return operations.findAllAndRemove(query, type, collection);
			}

			WriteResult writeResult = operations.remove(query, type, collection);
			return writeResult != null ? writeResult.getN() : 0L;
		}
	}

	/**
	 * @author Thomas Darimont
	 * @since 1.7
	 */
	@RequiredArgsConstructor
	static final class StreamExecution implements MongoQueryExecution {

		private final @NonNull MongoOperations operations;
		private final @NonNull Converter<Object, Object> resultProcessing;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.repository.query.AbstractMongoQuery.Execution#execute(org.springframework.data.mongodb.core.query.Query, java.lang.Class, java.lang.String)
		 */
		@Override
		@SuppressWarnings("unchecked")
		public Object execute(Query query, Class<?> type, String collection) {

			return StreamUtils.createStreamFromIterator((CloseableIterator<Object>) operations.stream(query, type))
					.map(new Function<Object, Object>() {

						@Override
						public Object apply(Object t) {
							return resultProcessing.convert(t);
						}
					});
		}
	}

	/**
	 * An {@link MongoQueryExecution} that wraps the results of the given delegate with the given result processing.
	 *
	 * @author Oliver Gierke
	 * @since 1.9
	 */
	@RequiredArgsConstructor
	static final class ResultProcessingExecution implements MongoQueryExecution {

		private final @NonNull MongoQueryExecution delegate;
		private final @NonNull Converter<Object, Object> converter;

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.repository.query.AbstractMongoQuery.Execution#execute(org.springframework.data.mongodb.core.query.Query, java.lang.Class, java.lang.String)
		 */
		@Override
		public Object execute(Query query, Class<?> type, String collection) {
			return converter.convert(delegate.execute(query, type, collection));
		}
	}

	/**
	 * A {@link Converter} to post-process all source objects using the given {@link ResultProcessor}.
	 *
	 * @author Oliver Gierke
	 * @since 1.9
	 */
	@RequiredArgsConstructor
	static final class ResultProcessingConverter implements Converter<Object, Object> {

		private final @NonNull ResultProcessor processor;
		private final @NonNull MongoOperations operations;
		private final @NonNull EntityInstantiators instantiators;

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public Object convert(Object source) {

			ReturnedType returnedType = processor.getReturnedType();

			if (ClassUtils.isPrimitiveOrWrapper(returnedType.getReturnedType())) {
				return source;
			}

			Converter<Object, Object> converter = new DtoInstantiatingConverter(returnedType.getReturnedType(),
					operations.getConverter().getMappingContext(), instantiators);

			return processor.processResult(source, converter);
		}
	}
}
