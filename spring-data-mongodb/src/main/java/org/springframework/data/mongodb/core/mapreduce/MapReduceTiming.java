/*
 * Copyright 2010-2022 the original author or authors.
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

import org.springframework.lang.Nullable;

/**
 * @deprecated since 3.4 in favor of {@link org.springframework.data.mongodb.core.aggregation}.
 */
@Deprecated
public class MapReduceTiming {

	private long mapTime, emitLoopTime, totalTime;

	public MapReduceTiming(long mapTime, long emitLoopTime, long totalTime) {

		this.mapTime = mapTime;
		this.emitLoopTime = emitLoopTime;
		this.totalTime = totalTime;
	}

	public long getMapTime() {
		return mapTime;
	}

	public long getEmitLoopTime() {
		return emitLoopTime;
	}

	public long getTotalTime() {
		return totalTime;
	}

	@Override
	public String toString() {
		return "MapReduceTiming [mapTime=" + mapTime + ", emitLoopTime=" + emitLoopTime + ", totalTime=" + totalTime + "]";
	}

	@Override
	public int hashCode() {

		final int prime = 31;
		int result = 1;

		result = prime * result + (int) (emitLoopTime ^ (emitLoopTime >>> 32));
		result = prime * result + (int) (mapTime ^ (mapTime >>> 32));
		result = prime * result + (int) (totalTime ^ (totalTime >>> 32));

		return result;
	}

	@Override
	public boolean equals(@Nullable Object obj) {

		if (this == obj) {
			return true;
		}

		if (!(obj instanceof MapReduceTiming)) {
			return false;
		}

		MapReduceTiming that = (MapReduceTiming) obj;

		return this.emitLoopTime == that.emitLoopTime && //
				this.mapTime == that.mapTime && //
				this.totalTime == that.totalTime;
	}
}
