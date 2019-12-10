/*
 * Copyright 2011-2019 the original author or authors.
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
package org.springframework.data.mongodb.core.mapreduce;

import java.util.Iterator;
import java.util.List;

import org.bson.Document;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Collects the results of performing a MapReduce operations.
 *
 * @author Mark Pollack
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 * @param <T> The class in which the results are mapped onto, accessible via an iterator.
 */
public class MapReduceResults<T> implements Iterable<T> {

	private final List<T> mappedResults;
	private final @Nullable Document rawResults;
	private final @Nullable String outputCollection;
	private final MapReduceTiming mapReduceTiming;
	private final MapReduceCounts mapReduceCounts;

	/**
	 * Creates a new {@link MapReduceResults} from the given mapped results and the raw one.
	 *
	 * @param mappedResults must not be {@literal null}.
	 * @param rawResults must not be {@literal null}.
	 */
	public MapReduceResults(List<T> mappedResults, Document rawResults) {

		Assert.notNull(mappedResults, "List of mapped results must not be null!");
		Assert.notNull(rawResults, "Raw results must not be null!");

		this.mappedResults = mappedResults;
		this.rawResults = rawResults;
		this.mapReduceTiming = parseTiming(rawResults);
		this.mapReduceCounts = parseCounts(rawResults);
		this.outputCollection = parseOutputCollection(rawResults);
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

	@Nullable
	public String getOutputCollection() {
		return outputCollection;
	}

	public Document getRawResults() {
		return rawResults;
	}

	private static MapReduceTiming parseTiming(Document rawResults) {

		Document timing = (Document) rawResults.get("timing");

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
	private static Long getAsLong(Document source, String key) {

		Object raw = source.get(key);

		return raw instanceof Long ? (Long) raw : (Integer) raw;
	}

	/**
	 * Parses the raw {@link Document} result into a {@link MapReduceCounts} value object.
	 *
	 * @param rawResults
	 * @return
	 */
	private static MapReduceCounts parseCounts(Document rawResults) {

		Document counts = (Document) rawResults.get("counts");

		if (counts == null) {
			return MapReduceCounts.NONE;
		}

		if (counts.get("input") != null && counts.get("emit") != null && counts.get("output") != null) {
			return new MapReduceCounts(getAsLong(counts, "input"), getAsLong(counts, "emit"), getAsLong(counts, "output"));
		}

		return MapReduceCounts.NONE;
	}

	/**
	 * Parses the output collection from the raw {@link Document} result.
	 *
	 * @param rawResults
	 * @return
	 */
	@Nullable
	private static String parseOutputCollection(Document rawResults) {

		Object resultField = rawResults.get("result");

		if (resultField == null) {
			return null;
		}

		return resultField instanceof Document ? ((Document) resultField).get("collection").toString()
				: resultField.toString();
	}
}
