/*
 * Copyright 2010-2013 the original author or authors.
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
package org.springframework.data.mongodb.core.query;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.util.Assert;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * @author Thomas Risberg
 * @author Oliver Gierke
 * @author Patryk Wasik
 */
public class Field {

	private final Map<String, Integer> criteria = new HashMap<String, Integer>();
	private final Map<String, Object> slices = new HashMap<String, Object>();
	private final Map<String, Criteria> elemMatchs = new HashMap<String, Criteria>();
	private String postionKey;
	private int positionValue;

	public Field include(String key) {
		criteria.put(key, Integer.valueOf(1));
		return this;
	}

	public Field exclude(String key) {
		criteria.put(key, Integer.valueOf(0));
		return this;
	}

	public Field slice(String key, int size) {
		slices.put(key, Integer.valueOf(size));
		return this;
	}

	public Field slice(String key, int offset, int size) {
		slices.put(key, new Integer[] { Integer.valueOf(offset), Integer.valueOf(size) });
		return this;
	}

	public Field elemMatch(String key, Criteria elemMatchCriteria) {
		elemMatchs.put(key, elemMatchCriteria);
		return this;
	}

	/**
	 * The array field must appear in the query. Only one positional {@code $} operator can appear in the projection and
	 * only one array field can appear in the query.
	 * 
	 * @param field query array field, must not be {@literal null} or empty.
	 * @param value
	 * @return
	 */
	public Field position(String field, int value) {

		Assert.hasText(field, "Field must not be null or empty!");

		postionKey = field;
		positionValue = value;

		return this;
	}

	public DBObject getFieldsObject() {

		DBObject dbo = new BasicDBObject();

		for (String k : criteria.keySet()) {
			dbo.put(k, criteria.get(k));
		}

		for (String k : slices.keySet()) {
			dbo.put(k, new BasicDBObject("$slice", slices.get(k)));
		}

		for (Entry<String, Criteria> entry : elemMatchs.entrySet()) {
			DBObject dbObject = new BasicDBObject("$elemMatch", entry.getValue().getCriteriaObject());
			dbo.put(entry.getKey(), dbObject);
		}

		if (postionKey != null) {
			dbo.put(postionKey + ".$", positionValue);
		}

		return dbo;
	}

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Field field = (Field) o;

        if (criteria != null ? !criteria.equals(field.criteria) : field.criteria != null) return false;
        if (slices != null ? !slices.equals(field.slices) : field.slices != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = criteria != null ? criteria.hashCode() : 0;
        result = 31 * result + (slices != null ? slices.hashCode() : 0);
        return result;
    }
}
