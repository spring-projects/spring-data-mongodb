/*
 * Copyright 2011 - 2016O the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *			http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.mapreduce;

import java.util.Iterator;
import java.util.List;

import org.bson.Document;
import org.springframework.util.Assert;

/**
 * Collects the results of executing a group operation.
 * 
 * @author Mark Pollack
 * @author Christoph Strobl
 * @param <T> The class in which the results are mapped onto, accessible via an {@link Iterator}.
 */
public class GroupByResults<T> implements Iterable<T> {

	private final List<T> mappedResults;
	private final Document rawResults;

	private double count;
	private int keys;
	private String serverUsed;

	public GroupByResults(List<T> mappedResults, Document rawResults) {

		Assert.notNull(mappedResults);
		Assert.notNull(rawResults);
		this.mappedResults = mappedResults;
		this.rawResults = rawResults;
		parseKeys();
		parseCount();
		parseServerUsed();
	}

	public double getCount() {
		return count;
	}

	public int getKeys() {
		return keys;
	}

	public String getServerUsed() {
		return serverUsed;
	}

	public Iterator<T> iterator() {
		return mappedResults.iterator();
	}

	public Document getRawResults() {
		return rawResults;
	}

	private void parseCount() {

		Object object = rawResults.get("count");
		if (object instanceof Number) {
			count = ((Number) object).doubleValue();
		}

	}

	private void parseKeys() {

		Object object = rawResults.get("keys");
		if (object instanceof Number) {
			keys = ((Number) object).intValue();
		}
	}

	private void parseServerUsed() {

		// "serverUsed" : "127.0.0.1:27017"
		Object object = rawResults.get("serverUsed");
		if (object instanceof String) {
			serverUsed = (String) object;
		}
	}
}
