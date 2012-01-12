/*
 * Copyright 2011 the original author or authors.
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

import org.springframework.util.Assert;

import com.mongodb.DBObject;

/**
 * Collects the results of executing a group operation.
 * 
 * @author Mark Pollack
 * @param <T> The class in which the results are mapped onto, accessible via an interator.
 */
public class GroupByResults<T> implements Iterable<T> {

	private final List<T> mappedResults;
	private final DBObject rawResults;

	private double count;
	private int keys;
	private String serverUsed;

	public GroupByResults(List<T> mappedResults, DBObject rawResults) {
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

	public DBObject getRawResults() {
		return rawResults;
	}

	private void parseCount() {
		Object object = rawResults.get("count");
		if (object instanceof Double) {
			count = (Double) object;
		}

	}

	private void parseKeys() {
		Object object = rawResults.get("keys");
		if (object instanceof Integer) {
			keys = (Integer) object;
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
