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
package org.springframework.data.document.mongodb.query;

import org.springframework.data.document.mongodb.index.IndexDefinition;
import org.springframework.util.Assert;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Value object to capture data to create a geo index.
 *
 * @author Jon Brisbin
 * @author Oliver Gierke
 */
public class GeospatialIndex implements IndexDefinition {

	private final String field;
	private String name;
	private Integer min = null;
	private Integer max = null;
	private Integer bits = null;

	/**
	 * Creates a new {@link GeospatialIndex} for the given field.
	 * 
	 * @param field must not be empty or {@literal null}.
	 */
	public GeospatialIndex(String field) {
		Assert.hasText(field);
		this.field = field;
	}

	public GeospatialIndex named(String name) {
		this.name = name;
		return this;
	}

	public GeospatialIndex withMin(int min) {
		this.min = Integer.valueOf(min);
		return this;
	}

	public GeospatialIndex withMax(int max) {
		this.max = Integer.valueOf(max);
		return this;
	}

	public GeospatialIndex withBits(int bits) {
		this.bits = Integer.valueOf(bits);
		return this;
	}

	public DBObject getIndexKeys() {
		DBObject dbo = new BasicDBObject();
		dbo.put(field, "2d");
		return dbo;
	}

	public DBObject getIndexOptions() {
		if (name == null && min == null && max == null) {
			return null;
		}
		DBObject dbo = new BasicDBObject();
		if (name != null) {
			dbo.put("name", name);
		}
		if (min != null) {
			dbo.put("min", min);
		}
		if (max != null) {
			dbo.put("max", max);
		}
		if (bits != null) {
			dbo.put("bits", bits);
		}
		return dbo;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format("Geo index: %s - Options: %s", getIndexKeys(), getIndexOptions());
	}
}
