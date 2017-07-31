/*
 * Copyright 2010-2017 the original author or authors.
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
package org.springframework.data.mongodb.core.mapreduce;

import java.util.Optional;

import org.bson.Document;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.lang.Nullable;

/**
 * Collects the parameters required to perform a group operation on a collection. The query condition and the input
 * collection are specified on the group method as method arguments to be consistent with other operations, e.g.
 * map-reduce.
 *
 * @author Mark Pollack
 * @author Christoph Strobl
 */
public class GroupBy {

	private @Nullable Document initialDocument;
	private @Nullable String reduce;

	private Optional<Document> keys = Optional.empty();
	private Optional<String> keyFunction = Optional.empty();
	private Optional<String> initial = Optional.empty();
	private Optional<String> finalize = Optional.empty();
	private Optional<Collation> collation = Optional.empty();

	public GroupBy(String... keys) {

		Document document = new Document();
		for (String key : keys) {
			document.put(key, 1);
		}

		this.keys = Optional.of(document);
	}

	// NOTE GroupByCommand does not handle keyfunction.

	public GroupBy(String key, boolean isKeyFunction) {

		Document document = new Document();
		if (isKeyFunction) {
			keyFunction = Optional.ofNullable(key);
		} else {
			document.put(key, 1);
			keys = Optional.of(document);
		}
	}

	/**
	 * Create new {@link GroupBy} with the field to group.
	 *
	 * @param key
	 * @return
	 */
	public static GroupBy keyFunction(String key) {
		return new GroupBy(key, true);
	}

	/**
	 * Create new {@link GroupBy} with the fields to group.
	 *
	 * @param keys
	 * @return
	 */
	public static GroupBy key(String... keys) {
		return new GroupBy(keys);
	}

	/**
	 * Define the aggregation result document.
	 *
	 * @param initialDocument can be {@literal null}.
	 * @return
	 */
	public GroupBy initialDocument(String initialDocument) {

		initial = Optional.ofNullable(initialDocument);
		return this;
	}

	/**
	 * Define the aggregation result document.
	 *
	 * @param initialDocument can be {@literal null}.
	 * @return
	 */
	public GroupBy initialDocument(Document initialDocument) {

		this.initialDocument = initialDocument;
		return this;
	}

	/**
	 * Define the aggregation function that operates on the documents during the grouping operation
	 *
	 * @param reduceFunction
	 * @return
	 */
	public GroupBy reduceFunction(String reduceFunction) {

		reduce = reduceFunction;
		return this;
	}

	/**
	 * Define the function that runs each item in the result set before db.collection.group() returns the final value.
	 *
	 * @param finalizeFunction
	 * @return
	 */
	public GroupBy finalizeFunction(String finalizeFunction) {

		finalize = Optional.ofNullable(finalizeFunction);
		return this;
	}

	/**
	 * Define the Collation specifying language-specific rules for string comparison.
	 *
	 * @param collation can be {@literal null}.
	 * @return
	 * @since 2.0
	 */
	public GroupBy collation(Collation collation) {

		this.collation = Optional.ofNullable(collation);
		return this;
	}

	/**
	 * Get the {@link Document} representation of the {@link GroupBy}.
	 *
	 * @return
	 */
	public Document getGroupByObject() {

		Document document = new Document();

		keys.ifPresent(val -> document.append("key", val));
		keyFunction.ifPresent(val -> document.append("$keyf", val));

		document.put("$reduce", reduce);
		document.put("initial", initialDocument);

		initial.ifPresent(val -> document.append("initial", val));
		finalize.ifPresent(val -> document.append("finalize", val));
		collation.ifPresent(val -> document.append("collation", val.toDocument()));

		return document;
	}

}
