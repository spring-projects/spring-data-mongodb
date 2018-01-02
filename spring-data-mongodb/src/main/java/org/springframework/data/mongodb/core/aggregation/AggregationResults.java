/*
 * Copyright 2013-2018 the original author or authors.
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
package org.springframework.data.mongodb.core.aggregation;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.bson.Document;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Collects the results of executing an aggregation operation.
 *
 * @author Tobias Trelle
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 * @param <T> The class in which the results are mapped onto.
 * @since 1.3
 */
public class AggregationResults<T> implements Iterable<T> {

	private final List<T> mappedResults;
	private final Document rawResults;
	private final @Nullable String serverUsed;

	/**
	 * Creates a new {@link AggregationResults} instance from the given mapped and raw results.
	 *
	 * @param mappedResults must not be {@literal null}.
	 * @param rawResults must not be {@literal null}.
	 */
	public AggregationResults(List<T> mappedResults, Document rawResults) {

		Assert.notNull(mappedResults, "List of mapped results must not be null!");
		Assert.notNull(rawResults, "Raw results must not be null!");

		this.mappedResults = Collections.unmodifiableList(mappedResults);
		this.rawResults = rawResults;
		this.serverUsed = parseServerUsed();
	}

	/**
	 * Returns the aggregation results.
	 *
	 * @return
	 */
	public List<T> getMappedResults() {
		return mappedResults;
	}

	/**
	 * Returns the unique mapped result. Assumes no result or exactly one.
	 *
	 * @return
	 * @throws IllegalArgumentException in case more than one result is available.
	 */
	@Nullable
	public T getUniqueMappedResult() {
		Assert.isTrue(mappedResults.size() < 2, "Expected unique result or null, but got more than one!");
		return mappedResults.size() == 1 ? mappedResults.get(0) : null;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Iterable#iterator()
	 */
	public Iterator<T> iterator() {
		return mappedResults.iterator();
	}

	/**
	 * Returns the server that has been used to perform the aggregation.
	 *
	 * @return
	 */
	@Nullable
	public String getServerUsed() {
		return serverUsed;
	}

	/**
	 * Returns the raw result that was returned by the server.
	 *
	 * @return
	 * @since 1.6
	 */
	public Document getRawResults() {
		return rawResults;
	}

	@Nullable
	private String parseServerUsed() {

		Object object = rawResults.get("serverUsed");
		return object instanceof String ? (String) object : null;
	}
}
