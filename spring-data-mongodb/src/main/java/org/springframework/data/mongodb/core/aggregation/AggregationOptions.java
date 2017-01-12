/*
 * Copyright 2014-2016 the original author or authors.
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

import org.bson.Document;

/**
 * Holds a set of configurable aggregation options that can be used within an aggregation pipeline. A list of support
 * aggregation options can be found in the MongoDB reference documentation
 * https://docs.mongodb.org/manual/reference/command/aggregate/#aggregate
 * 
 * @author Thomas Darimont
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @see Aggregation#withOptions(AggregationOptions)
 * @see TypedAggregation#withOptions(AggregationOptions)
 * @since 1.6
 */
public class AggregationOptions {

	private static final String CURSOR = "cursor";
	private static final String EXPLAIN = "explain";
	private static final String ALLOW_DISK_USE = "allowDiskUse";

	private final boolean allowDiskUse;
	private final boolean explain;
	private final Document cursor;

	/**
	 * Creates a new {@link AggregationOptions}.
	 * 
	 * @param allowDiskUse whether to off-load intensive sort-operations to disk.
	 * @param explain whether to get the execution plan for the aggregation instead of the actual results.
	 * @param cursor can be {@literal null}, used to pass additional options to the aggregation.
	 */
	public AggregationOptions(boolean allowDiskUse, boolean explain, Document cursor) {

		this.allowDiskUse = allowDiskUse;
		this.explain = explain;
		this.cursor = cursor;
	}

	/**
	 * Enables writing to temporary files. When set to true, aggregation stages can write data to the _tmp subdirectory in
	 * the dbPath directory.
	 * 
	 * @return
	 */
	public boolean isAllowDiskUse() {
		return allowDiskUse;
	}

	/**
	 * Specifies to return the information on the processing of the pipeline.
	 * 
	 * @return
	 */
	public boolean isExplain() {
		return explain;
	}

	/**
	 * Specify a document that contains options that control the creation of the cursor object.
	 * 
	 * @return
	 */
	public Document getCursor() {
		return cursor;
	}

	/**
	 * Returns a new potentially adjusted copy for the given {@code aggregationCommandObject} with the configuration
	 * applied.
	 * 
	 * @param command the aggregation command.
	 * @return
	 */
	Document applyAndReturnPotentiallyChangedCommand(Document command) {

		Document result = new Document(command);

		if (allowDiskUse && !result.containsKey(ALLOW_DISK_USE)) {
			result.put(ALLOW_DISK_USE, allowDiskUse);
		}

		if (explain && !result.containsKey(EXPLAIN)) {
			result.put(EXPLAIN, explain);
		}

		if (cursor != null && !result.containsKey(CURSOR)) {
			result.put("cursor", cursor);
		}

		return result;
	}

	/**
	 * Returns a {@link Document} representation of this {@link AggregationOptions}.
	 * 
	 * @return
	 */
	public Document toDocument() {

		Document document = new Document();
		document.put(ALLOW_DISK_USE, allowDiskUse);
		document.put(EXPLAIN, explain);
		document.put(CURSOR, cursor);

		return document;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return toDocument().toJson();
	}

	/**
	 * A Builder for {@link AggregationOptions}.
	 * 
	 * @author Thomas Darimont
	 */
	public static class Builder {

		private boolean allowDiskUse;
		private boolean explain;
		private Document cursor;

		/**
		 * Defines whether to off-load intensive sort-operations to disk.
		 * 
		 * @param allowDiskUse
		 * @return
		 */
		public Builder allowDiskUse(boolean allowDiskUse) {

			this.allowDiskUse = allowDiskUse;
			return this;
		}

		/**
		 * Defines whether to get the execution plan for the aggregation instead of the actual results.
		 * 
		 * @param explain
		 * @return
		 */
		public Builder explain(boolean explain) {

			this.explain = explain;
			return this;
		}

		/**
		 * Additional options to the aggregation.
		 * 
		 * @param cursor
		 * @return
		 */
		public Builder cursor(Document cursor) {

			this.cursor = cursor;
			return this;
		}

		/**
		 * Returns a new {@link AggregationOptions} instance with the given configuration.
		 * 
		 * @return
		 */
		public AggregationOptions build() {
			return new AggregationOptions(allowDiskUse, explain, cursor);
		}
	}
}
