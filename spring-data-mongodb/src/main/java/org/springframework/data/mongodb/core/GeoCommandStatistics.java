/*
 * Copyright 2016-2017 the original author or authors.
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
package org.springframework.data.mongodb.core;

import org.springframework.util.Assert;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Value object to mitigate different representations of geo command execution results in MongoDB.
 * 
 * @author Oliver Gierke
 * @soundtrack Fruitcake - Jeff Coffin (The Inside of the Outside)
 */
class GeoCommandStatistics {

	private static final GeoCommandStatistics NONE = new GeoCommandStatistics(new BasicDBObject());

	private final DBObject source;

	/**
	 * Creates a new {@link GeoCommandStatistics} instance with the given source document.
	 * 
	 * @param source must not be {@literal null}.
	 */
	private GeoCommandStatistics(DBObject source) {

		Assert.notNull(source, "Source document must not be null!");
		this.source = source;
	}

	/**
	 * Creates a new {@link GeoCommandStatistics} from the given command result extracting the statistics.
	 * 
	 * @param commandResult must not be {@literal null}.
	 * @return
	 */
	public static GeoCommandStatistics from(DBObject commandResult) {

		Assert.notNull(commandResult, "Command result must not be null!");

		Object stats = commandResult.get("stats");
		return stats == null ? NONE : new GeoCommandStatistics((DBObject) stats);
	}

	/**
	 * Returns the average distance reported by the command result. Mitigating a removal of the field in case the command
	 * didn't return any result introduced in MongoDB 3.2 RC1.
	 * 
	 * @return
	 * @see <a href="https://jira.mongodb.org/browse/SERVER-21024">MongoDB Jira SERVER-21024</a>
	 */
	public double getAverageDistance() {

		Object averageDistance = source.get("avgDistance");
		return averageDistance == null ? Double.NaN : (Double) averageDistance;
	}
}
