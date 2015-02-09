/*
 * Copyright 2011-2015 the original author or authors.
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

import static org.springframework.util.Assert.*;

import java.util.Iterator;
import java.util.List;

import com.mongodb.DBObject;
import com.mongodb.MapReduceOutput;

/**
 * Collects the results of performing a MapReduce operations.
 * 
 * @author Mark Pollack
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @param <T> The class in which the results are mapped onto, accessible via an iterator.
 */
public class MapReduceResults<T> implements Iterable<T> {

	private final List<T> mappedResults;
	private final DBObject rawResults;
	private final String outputCollection;
	private final MapReduceTiming mapReduceTiming;
	private final MapReduceCounts mapReduceCounts;

	/**
	 * Creates a new {@link MapReduceResults} from the given mapped results and the raw one.
	 * 
	 * @param mappedResults must not be {@literal null}.
	 * @param rawResults must not be {@literal null}.
	 * @deprecated since 1.7. Please use {@link #MapReduceResults(List, MapReduceOutput)}
	 */
	@Deprecated
	public MapReduceResults(List<T> mappedResults, DBObject rawResults) {

		notNull(mappedResults);
		notNull(rawResults);

		this.mappedResults = mappedResults;
		this.rawResults = rawResults;
		this.mapReduceTiming = parseTiming(rawResults);
		this.mapReduceCounts = parseCounts(rawResults);
		this.outputCollection = parseOutputCollection(rawResults);
	}

	/**
	 * Creates a new {@link MapReduceResults} from the given mapped results and the {@link MapReduceOutput}.
	 * 
	 * @param mappedResults must not be {@literal null}.
	 * @param mapReduceOutput must not be {@literal null}.
	 * @since 1.7
	 */
	public MapReduceResults(List<T> mappedResults, MapReduceOutput mapReduceOutput) {

		notNull(mappedResults, "MappedResults must not be null!");
		notNull(mapReduceOutput, "MapReduceOutput must not be null!");

		this.mappedResults = mappedResults;
		this.rawResults = null;
		this.mapReduceTiming = parseTiming(mapReduceOutput);
		this.mapReduceCounts = parseCounts(mapReduceOutput);
		this.outputCollection = parseOutputCollection(mapReduceOutput);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Iterable#iterator()
	 */
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

	private MapReduceTiming parseTiming(DBObject rawResults) {

		DBObject timing = (DBObject) rawResults.get("timing");

		if (timing == null) {
			return new MapReduceTiming(-1, -1, -1);
		}

		if (timing.get("mapTime") != null && timing.get("emitLoop") != null && timing.get("total") != null) {
			return new MapReduceTiming(getAsLong(timing, "mapTime"), getAsLong(timing, "emitLoop"),
					getAsLong(timing, "total"));
		}

		return new MapReduceTiming(-1, -1, -1);
	}

	/**
	 * Returns the value of the source's field with the given key as {@link Long}.
	 * 
	 * @param source
	 * @param key
	 * @return
	 */
	private Long getAsLong(DBObject source, String key) {
		Object raw = source.get(key);
		return raw instanceof Long ? (Long) raw : (Integer) raw;
	}

	/**
	 * Parses the raw {@link DBObject} result into a {@link MapReduceCounts} value object.
	 * 
	 * @param rawResults
	 * @return
	 */
	private MapReduceCounts parseCounts(DBObject rawResults) {

		DBObject counts = (DBObject) rawResults.get("counts");

		if (counts == null) {
			return MapReduceCounts.NONE;
		}

		if (counts.get("input") != null && counts.get("emit") != null && counts.get("output") != null) {
			return new MapReduceCounts(getAsLong(counts, "input"), getAsLong(counts, "emit"), getAsLong(counts, "output"));
		}

		return MapReduceCounts.NONE;
	}

	/**
	 * Parses the output collection from the raw {@link DBObject} result.
	 * 
	 * @param rawResults
	 * @return
	 */
	private String parseOutputCollection(DBObject rawResults) {

		Object resultField = rawResults.get("result");

		if (resultField == null) {
			return null;
		}

		return resultField instanceof DBObject ? ((DBObject) resultField).get("collection").toString() : resultField
				.toString();
	}

	private MapReduceCounts parseCounts(final MapReduceOutput mapReduceOutput) {
		return new MapReduceCounts(mapReduceOutput.getInputCount(), mapReduceOutput.getEmitCount(),
				mapReduceOutput.getOutputCount());
	}

	private String parseOutputCollection(final MapReduceOutput mapReduceOutput) {
		return mapReduceOutput.getCollectionName();
	}

	private MapReduceTiming parseTiming(MapReduceOutput mapReduceOutput) {
		return new MapReduceTiming(-1, -1, mapReduceOutput.getDuration());
	}
}
