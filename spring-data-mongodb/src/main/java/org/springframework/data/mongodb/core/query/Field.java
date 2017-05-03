/*
 * Copyright 2010-2016 the original author or authors.
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

import lombok.EqualsAndHashCode;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.bson.Document;
import org.springframework.util.Assert;

/**
 * @author Thomas Risberg
 * @author Oliver Gierke
 * @author Patryk Wasik
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@EqualsAndHashCode
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

		Assert.hasText(field, "DocumentField must not be null or empty!");

		postionKey = field;
		positionValue = value;

		return this;
	}

	public Document getFieldsObject() {

		@SuppressWarnings({ "unchecked", "rawtypes" })
		Document document = new Document((Map) criteria);

		for (Entry<String, Object> entry : slices.entrySet()) {
			document.put(entry.getKey(), new Document("$slice", entry.getValue()));
		}

		for (Entry<String, Criteria> entry : elemMatchs.entrySet()) {
			document.put(entry.getKey(), new Document("$elemMatch", entry.getValue().getCriteriaObject()));
		}

		if (postionKey != null) {
			document.put(postionKey + ".$", positionValue);
		}

		return document;
	}
}
