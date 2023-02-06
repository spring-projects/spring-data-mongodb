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

import java.util.List;

import org.bson.Document;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoCollection;

/**
 * Delegate to apply {@link ReadConcern} and {@link ReadPreference} settings upon {@link CollectionPreparer preparing a
 * collection}.
 *
 * @author Mark Paluch
 * @since 4.1
 */
class CollectionPreparerDelegate implements ReadConcernAware, ReadPreferenceAware, CollectionPreparer {

	List<Object> sources;

	private CollectionPreparerDelegate(List<Object> sources) {
		this.sources = sources;
	}

	public static CollectionPreparerDelegate of(ReadPreferenceAware... awares) {
		return of((Object[]) awares);
	}

	public static CollectionPreparerDelegate of(Object... mixedAwares) {

		if (mixedAwares.length == 1 && mixedAwares[0] instanceof CollectionPreparerDelegate) {
			return (CollectionPreparerDelegate) mixedAwares[0];
		}
		return new CollectionPreparerDelegate(List.of(mixedAwares));
	}

	@Override
	public MongoCollection<Document> prepare(MongoCollection<Document> collection) {

		MongoCollection<Document> collectionToUse = collection;

		for (Object source : sources) {
			if (source instanceof ReadConcernAware rca && rca.hasReadConcern()) {

				ReadConcern concern = rca.getReadConcern();
				if (collection.getReadConcern() != concern) {
					collectionToUse = collectionToUse.withReadConcern(concern);
				}
				break;
			}
		}

		for (Object source : sources) {
			if (source instanceof ReadPreferenceAware rpa && rpa.hasReadPreference()) {

				ReadPreference preference = rpa.getReadPreference();
				if (collection.getReadPreference() != preference) {
					collectionToUse = collectionToUse.withReadPreference(preference);
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

}
