/*
 * Copyright 2010-2011 the original author or authors.
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

public class MapReduceTiming {

	private long mapTime;
	
	private long emitLoopTime;
	
	private long totalTime;
	
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
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MapReduceTiming other = (MapReduceTiming) obj;
		if (emitLoopTime != other.emitLoopTime)
			return false;
		if (mapTime != other.mapTime)
			return false;
		if (totalTime != other.totalTime)
			return false;
		return true;
	}
	
	
	
	
}
