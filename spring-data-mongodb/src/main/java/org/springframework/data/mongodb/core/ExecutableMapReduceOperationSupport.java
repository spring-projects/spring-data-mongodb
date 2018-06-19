/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.mongodb.core;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.List;

import org.springframework.data.mongodb.core.mapreduce.MapReduceOptions;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Implementation of {@link ExecutableMapReduceOperation}.
 *
 * @author Christoph Strobl
 * @since 2.1
 */
@RequiredArgsConstructor
class ExecutableMapReduceOperationSupport implements ExecutableMapReduceOperation {

	private static final Query ALL_QUERY = new Query();

	private final @NonNull MongoTemplate template;

	/*
	 * (non-Javascript)
	 * @see in org.springframework.data.mongodb.core.ExecutableMapReduceOperation#mapReduce(java.lang.Class)
	 */
	@Override
	public <T> ExecutableMapReduceSupport<T> mapReduce(Class<T> domainType) {

		Assert.notNull(domainType, "DomainType must not be null!");

		return new ExecutableMapReduceSupport<>(template, domainType, domainType, null, ALL_QUERY, null, null, null);
	}

	/**
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	static class ExecutableMapReduceSupport<T>
			implements ExecutableMapReduce<T>, MapReduceWithOptions<T>, MapReduceWithCollection<T>,
			MapReduceWithProjection<T>, MapReduceWithQuery<T>, MapReduceWithReduceFunction<T>, MapReduceWithMapFunction<T> {

		private final MongoTemplate template;
		private final Class<?> domainType;
		private final Class<T> returnType;
		private final @Nullable String collection;
		private final Query query;
		private final @Nullable String mapFunction;
		private final @Nullable String reduceFunction;
		private final @Nullable MapReduceOptions options;

		ExecutableMapReduceSupport(MongoTemplate template, Class<?> domainType, Class<T> returnType,
				@Nullable String collection, Query query, @Nullable String mapFunction, @Nullable String reduceFunction,
				@Nullable MapReduceOptions options) {

			this.template = template;
			this.domainType = domainType;
			this.returnType = returnType;
			this.collection = collection;
			this.query = query;
			this.mapFunction = mapFunction;
			this.reduceFunction = reduceFunction;
			this.options = options;
		}

		/*
		 * (non-Javascript)
		 * @see in org.springframework.data.mongodb.core.ExecutableMapReduceOperation.TerminatingMapReduce#all()
		 */
		@Override
		public List<T> all() {
			return template.mapReduce(query, domainType, getCollectionName(), mapFunction, reduceFunction, options,
					returnType);
		}

		/*
		 * (non-Javascript)
		 * @see in org.springframework.data.mongodb.core.ExecutableMapReduceOperation.MapReduceWithCollection#inCollection(java.lang.String)
		 */
		@Override
		public MapReduceWithProjection<T> inCollection(String collection) {

			Assert.hasText(collection, "Collection name must not be null nor empty!");

			return new ExecutableMapReduceSupport<>(template, domainType, returnType, collection, query, mapFunction,
					reduceFunction, options);
		}

		/*
		 * (non-Javascript)
		 * @see in org.springframework.data.mongodb.core.ExecutableMapReduceOperation.MapReduceWithQuery#query(org.springframework.data.mongodb.core.query.Query)
		 */
		@Override
		public TerminatingMapReduce<T> matching(Query query) {

			Assert.notNull(query, "Query must not be null!");

			return new ExecutableMapReduceSupport<>(template, domainType, returnType, collection, query, mapFunction,
					reduceFunction, options);
		}

		/*
		 * (non-Javascript)
		 * @see in org.springframework.data.mongodb.core.ExecutableMapReduceOperation.MapReduceWithProjection#as(java.lang.Class)
		 */
		@Override
		public <R> MapReduceWithQuery<R> as(Class<R> resultType) {

			Assert.notNull(resultType, "ResultType must not be null!");

			return new ExecutableMapReduceSupport<>(template, domainType, resultType, collection, query, mapFunction,
					reduceFunction, options);
		}

		/*
		 * (non-Javascript)
		 * @see in org.springframework.data.mongodb.core.ExecutableMapReduceOperation.MapReduceWithOptions#with(org.springframework.data.mongodb.core.mapreduce.MapReduceOptions)
		 */
		@Override
		public ExecutableMapReduce<T> with(MapReduceOptions options) {

			Assert.notNull(options, "Options must not be null! Please consider empty MapReduceOptions#options() instead.");

			return new ExecutableMapReduceSupport<>(template, domainType, returnType, collection, query, mapFunction,
					reduceFunction, options);
		}

		/*
		 * (non-Javascript)
		 * @see in org.springframework.data.mongodb.core.ExecutableMapReduceOperation.MapReduceWithMapFunction#map(java.lang.String)
		 */
		@Override
		public MapReduceWithReduceFunction<T> map(String mapFunction) {

			Assert.hasText(mapFunction, "MapFunction name must not be null nor empty!");

			return new ExecutableMapReduceSupport<>(template, domainType, returnType, collection, query, mapFunction,
					reduceFunction, options);
		}

		/*
		 * (non-Javascript)
		 * @see in org.springframework.data.mongodb.core.ExecutableMapReduceOperation.MapReduceWithReduceFunction#reduce(java.lang.String)
		 */
		@Override
		public ExecutableMapReduce<T> reduce(String reduceFunction) {

			Assert.hasText(reduceFunction, "ReduceFunction name must not be null nor empty!");

			return new ExecutableMapReduceSupport<>(template, domainType, returnType, collection, query, mapFunction,
					reduceFunction, options);
		}

		private String getCollectionName() {
			return StringUtils.hasText(collection) ? collection : template.getCollectionName(domainType);
		}
	}
}
