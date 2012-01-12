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
 * Collects the results of performing a MapReduce operations.
 * 
 * @author Mark Pollack
 * 
 * @param <T> The class in which the results are mapped onto, accessible via an interator.
 */
public class MapReduceResults<T> implements Iterable<T> {

	private final List<T> mappedResults;

	private DBObject rawResults;

	private MapReduceTiming mapReduceTiming;

	private MapReduceCounts mapReduceCounts;

	private String outputCollection;

	public MapReduceResults(List<T> mappedResults, DBObject rawResults) {
		Assert.notNull(mappedResults);
		Assert.notNull(rawResults);
		this.mappedResults = mappedResults;
		this.rawResults = rawResults;
		parseTiming(rawResults);
		parseCounts(rawResults);
		if (rawResults.get("result") != null) {
			this.outputCollection = (String) rawResults.get("result");
		}
	}

	public Iterator<T> iterator() {
		return mappedResults.iterator();
	}

	public MapReduceTiming getTiming() {
		return mapReduceTiming;
	}

	public MapReduceCounts getCounts() {
		return mapReduceCounts;
	}

	public String getOutputCollection() {
		return outputCollection;
	}

	public DBObject getRawResults() {
		return rawResults;
	}

	protected void parseTiming(DBObject rawResults) {
		DBObject timing = (DBObject) rawResults.get("timing");
		if (timing != null) {
			if (timing.get("mapTime") != null && timing.get("emitLoop") != null && timing.get("total") != null) {
				mapReduceTiming = new MapReduceTiming((Long) timing.get("mapTime"), (Integer) timing.get("emitLoop"),
						(Integer) timing.get("total"));
			}
		} else {
			mapReduceTiming = new MapReduceTiming(-1, -1, -1);
		}
	}

	protected void parseCounts(DBObject rawResults) {
		DBObject counts = (DBObject) rawResults.get("counts");
		if (counts != null) {
			if (counts.get("input") != null && counts.get("emit") != null && counts.get("output") != null) {
				mapReduceCounts = new MapReduceCounts((Integer) counts.get("input"), (Integer) counts.get("emit"),
						(Integer) counts.get("output"));
			}
		} else {
			mapReduceCounts = new MapReduceCounts(-1, -1, -1);
		}
	}

}
