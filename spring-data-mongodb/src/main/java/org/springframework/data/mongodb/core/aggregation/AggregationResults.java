/*
 * Copyright 2011-2012 the original author or authors.
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.springframework.util.Assert;

import com.mongodb.DBObject;

/**
 * Collects the results of executing an aggregation operation.
 * 
 * @author Tobias Trelle
 * 
 * @param <T> The class in which the results are mapped onto.
 */
public class AggregationResults<T> implements Iterable<T> {

	private final List<T> mappedResults;
	private final DBObject rawResults;

	private String serverUsed;
	
	public AggregationResults(List<T> mappedResults, DBObject rawResults) {
		Assert.notNull(mappedResults);
		Assert.notNull(rawResults);
		this.mappedResults = mappedResults;
		this.rawResults = rawResults;
		parseServerUsed();
	}

	public List<T> getAggregationResult() {
		List<T> result = new ArrayList<T>();
		Iterator<T> it = iterator();

		while (it.hasNext()) {
			result.add(it.next());
		}

		return result;
	}

	@Override
	public Iterator<T> iterator() {
		return mappedResults.iterator();
	}
	
	public String getServerUsed() {
		return serverUsed;
	}

	private void parseServerUsed() {
		Object object = rawResults.get("serverUsed");
		if (object instanceof String) {
			serverUsed = (String) object;
		}
	}
	
}
