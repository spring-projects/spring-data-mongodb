/*
 * Copyright 2010-2018 the original author or authors.
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

/**
 * Value object to encapsulate results of a map-reduce count.
 *
 * @author Mark Pollack
 * @author Oliver Gierke
 */
public class MapReduceCounts {

	public static final MapReduceCounts NONE = new MapReduceCounts(-1, -1, -1);

	private final long inputCount;
	private final long emitCount;
	private final long outputCount;

	/**
	 * Creates a new {@link MapReduceCounts} using the given input count, emit count, and output count.
	 *
	 * @param inputCount
	 * @param emitCount
	 * @param outputCount
	 */
	public MapReduceCounts(long inputCount, long emitCount, long outputCount) {

		this.inputCount = inputCount;
		this.emitCount = emitCount;
		this.outputCount = outputCount;
	}

	public long getInputCount() {
		return inputCount;
	}

	public long getEmitCount() {
		return emitCount;
	}

	public long getOutputCount() {
		return outputCount;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "MapReduceCounts [inputCount=" + inputCount + ", emitCount=" + emitCount + ", outputCount=" + outputCount
				+ "]";
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {

		final int prime = 31;
		long result = 1;

		result = prime * result + emitCount;
		result = prime * result + inputCount;
		result = prime * result + outputCount;

		return Long.valueOf(result).intValue();
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		MapReduceCounts other = (MapReduceCounts) obj;
		if (emitCount != other.emitCount) {
			return false;
		}
		if (inputCount != other.inputCount) {
			return false;
		}
		if (outputCount != other.outputCount) {
			return false;
		}
		return true;
	}
}
