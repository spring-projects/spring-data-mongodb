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
package org.springframework.data.document.mongodb.builder;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class UpdateSpec implements Update {

	public enum Position {
		LAST, FIRST
	}

	private HashMap<String, Object> criteria = new LinkedHashMap<String, Object>();
	
	public UpdateSpec set(String key, Object value) {
		criteria.put("$set", Collections.singletonMap(key, value));
		return this;
	}

	public UpdateSpec unset(String key) {
		criteria.put("$unset", Collections.singletonMap(key, 1));
		return this;
	}

	public UpdateSpec inc(String key, long inc) {
		criteria.put("$inc", Collections.singletonMap(key, inc));
		return this;
	}

	public UpdateSpec push(String key, Object value) {
		criteria.put("$push", Collections.singletonMap(key, value));
		return this;
	}

	public UpdateSpec pushAll(String key, Object[] values) {
		DBObject keyValue = new BasicDBObject();
		keyValue.put(key, values);
		criteria.put("$pushAll", keyValue);
		return this;
	}

	public UpdateSpec addToSet(String key, Object value) {
		criteria.put("$addToSet", Collections.singletonMap(key, value));
		return this;
	}

	public UpdateSpec pop(String key, Position pos) {
		criteria.put("$pop", Collections.singletonMap(key, (pos == Position.FIRST ? -1 : 1)));
		return this;
	}

	public UpdateSpec pull(String key, Object value) {
		criteria.put("$pull", Collections.singletonMap(key, value));
		return this;
	}

	public UpdateSpec pullAll(String key, Object[] values) {
		DBObject keyValue = new BasicDBObject();
		keyValue.put(key, values);
		criteria.put("$pullAll", keyValue);
		return this;
	}

	public UpdateSpec rename(String oldName, String newName) {
		criteria.put("$rename", Collections.singletonMap(oldName, newName));
		return this;
	}

	public Update build() {
		return this;
	}

	public DBObject getUpdateObject() {
		DBObject dbo = new BasicDBObject();
		for (String k : criteria.keySet()) {
			dbo.put(k, criteria.get(k));
		}
		return dbo;
	}

}
