/*
 * Copyright 2023 the original author or authors.
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
package org.springframework.data.mongodb.core;

import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.bson.Document;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoCollection;

/**
 * Support class for delegate implementations to apply {@link ReadConcern} and {@link ReadPreference} settings upon
 * {@link CollectionPreparer preparing a collection}.
 *
 * @author Mark Paluch
 * @since 4.1
 */
class CollectionPreparerSupport implements ReadConcernAware, ReadPreferenceAware {

	private final List<Object> sources;

	private CollectionPreparerSupport(List<Object> sources) {
		this.sources = sources;
	}

	<T> T doPrepare(T collection, Function<T, ReadConcern> concernAccessor, BiFunction<T, ReadConcern, T> concernFunction,
			Function<T, ReadPreference> preferenceAccessor, BiFunction<T, ReadPreference, T> preferenceFunction) {

		T collectionToUse = collection;

		for (Object source : sources) {
			if (source instanceof ReadConcernAware rca && rca.hasReadConcern()) {

				ReadConcern concern = rca.getReadConcern();
				if (concernAccessor.apply(collectionToUse) != concern) {
					collectionToUse = concernFunction.apply(collectionToUse, concern);
				}
				break;
			}
		}

		for (Object source : sources) {
			if (source instanceof ReadPreferenceAware rpa && rpa.hasReadPreference()) {

				ReadPreference preference = rpa.getReadPreference();
				if (preferenceAccessor.apply(collectionToUse) != preference) {
					collectionToUse = preferenceFunction.apply(collectionToUse, preference);
				}
				break;
			}
		}

		return collectionToUse;
	}

	@Override
	public boolean hasReadConcern() {

		for (Object aware : sources) {
			if (aware instanceof ReadConcernAware rca && rca.hasReadConcern()) {
				return true;
			}
		}

		return false;
	}

	@Override
	public ReadConcern getReadConcern() {

		for (Object aware : sources) {
			if (aware instanceof ReadConcernAware rca && rca.hasReadConcern()) {
				return rca.getReadConcern();
			}
		}

		return null;
	}

	@Override
	public boolean hasReadPreference() {

		for (Object aware : sources) {
			if (aware instanceof ReadPreferenceAware rpa && rpa.hasReadPreference()) {
				return true;
			}
		}

		return false;
	}

	@Override
	public ReadPreference getReadPreference() {

		for (Object aware : sources) {
			if (aware instanceof ReadPreferenceAware rpa && rpa.hasReadPreference()) {
				return rpa.getReadPreference();
			}
		}

		return null;
	}

	static class CollectionPreparerDelegate extends CollectionPreparerSupport
			implements CollectionPreparer<MongoCollection<Document>> {

		private CollectionPreparerDelegate(List<Object> sources) {
			super(sources);
		}

		public static CollectionPreparerDelegate of(ReadPreferenceAware... awares) {
			return of((Object[]) awares);
		}

		public static CollectionPreparerDelegate of(Object... mixedAwares) {

			if (mixedAwares.length == 1 && mixedAwares[0] instanceof CollectionPreparerDelegate) {
				return (CollectionPreparerDelegate) mixedAwares[0];
			}

			return new CollectionPreparerDelegate(Arrays.asList(mixedAwares));
		}

		@Override
		public MongoCollection<Document> prepare(MongoCollection<Document> collection) {
			return doPrepare(collection, MongoCollection::getReadConcern, MongoCollection::withReadConcern,
					MongoCollection::getReadPreference, MongoCollection::withReadPreference);
		}

	}

	static class ReactiveCollectionPreparerDelegate extends CollectionPreparerSupport
			implements CollectionPreparer<com.mongodb.reactivestreams.client.MongoCollection<Document>> {

		private ReactiveCollectionPreparerDelegate(List<Object> sources) {
			super(sources);
		}

		public static ReactiveCollectionPreparerDelegate of(ReadPreferenceAware... awares) {
			return of((Object[]) awares);
		}

		public static ReactiveCollectionPreparerDelegate of(Object... mixedAwares) {

			if (mixedAwares.length == 1 && mixedAwares[0] instanceof CollectionPreparerDelegate) {
				return (ReactiveCollectionPreparerDelegate) mixedAwares[0];
			}

			return new ReactiveCollectionPreparerDelegate(Arrays.asList(mixedAwares));
		}

		@Override
		public com.mongodb.reactivestreams.client.MongoCollection<Document> prepare(
				com.mongodb.reactivestreams.client.MongoCollection<Document> collection) {
			return doPrepare(collection, //
					com.mongodb.reactivestreams.client.MongoCollection::getReadConcern,
					com.mongodb.reactivestreams.client.MongoCollection::withReadConcern,
					com.mongodb.reactivestreams.client.MongoCollection::getReadPreference,
					com.mongodb.reactivestreams.client.MongoCollection::withReadPreference);
		}

	}

}
