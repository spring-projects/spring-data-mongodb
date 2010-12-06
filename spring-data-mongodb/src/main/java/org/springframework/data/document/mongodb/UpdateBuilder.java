/*
 * Copyright 2010 the original author or authors.
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
package org.springframework.data.document.mongodb;

import java.util.Collections;
import java.util.LinkedHashMap;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class UpdateBuilder {
	
	private LinkedHashMap<String, Object> criteria = new LinkedHashMap<String, Object>();

	public UpdateBuilder set(String key, Object value) {
		criteria.put("$set", Collections.singletonMap(key, value));
		return this;
	}

	public UpdateBuilder unset(String key) {
		criteria.put("$unset", Collections.singletonMap(key, 1));
		return this;
	}

	public UpdateBuilder inc(String key, long inc) {
		criteria.put("$inc", Collections.singletonMap(key, inc));
		return this;
	}

	public DBObject build() {
		DBObject dbo = new BasicDBObject();
		for (String k : criteria.keySet()) {
			dbo.put(k, criteria.get(k));
		}
		return dbo;
	}

}
