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

import com.mongodb.ReadPreference;
import com.mongodb.client.MongoCollection;

/**
 * @author Mark Paluch
 * @since 4.1
 */
class ReadPreferenceDelegate implements ReadPreferenceAware, CollectionPreparer {

	List<ReadPreferenceAware> readPreferences;

	private ReadPreferenceDelegate(List<ReadPreferenceAware> readPreferences) {
		this.readPreferences = readPreferences;
	}

	public static ReadPreferenceDelegate of(ReadPreferenceAware... awares) {

		if (awares.length == 1 && awares[0] instanceof ReadPreferenceDelegate) {
			return (ReadPreferenceDelegate) awares[0];
		}
		return new ReadPreferenceDelegate(List.of(awares));
	}

	@Override
	public MongoCollection<Document> prepare(MongoCollection<Document> collection) {

		for (ReadPreferenceAware readPreference : readPreferences) {
			if (readPreference.hasReadPreference()) {

				ReadPreference rp = readPreference.getReadPreference();
				if (collection.getReadPreference() != rp) {
					return collection.withReadPreference(rp);
				}
			}
		}

		return collection;
	}

	@Override
	public boolean hasReadPreference() {

		for (ReadPreferenceAware aware : readPreferences) {
			if (aware.hasReadPreference()) {
				return true;
			}
		}
		return false;
	}

	@Override
	public ReadPreference getReadPreference() {

		for (ReadPreferenceAware aware : readPreferences) {
			if (aware.hasReadPreference()) {
				return aware.getReadPreference();
			}

		}

		return null;
	}

}
