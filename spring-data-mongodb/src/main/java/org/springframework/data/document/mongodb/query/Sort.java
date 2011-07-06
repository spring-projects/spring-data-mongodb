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

import java.util.LinkedHashMap;
import java.util.Map;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class Sort {

	private Map<String, Order> fieldSpec = new LinkedHashMap<String, Order>();

	public Sort() {
	}

	public Sort(String key, Order order) {
		fieldSpec.put(key, order);
	}

	public Sort on(String key, Order order) {
		fieldSpec.put(key, order);
		return this;
	}

	public DBObject getSortObject() {
		DBObject dbo = new BasicDBObject();
		for (String k : fieldSpec.keySet()) {
			dbo.put(k, (fieldSpec.get(k).equals(Order.ASCENDING) ? 1 : -1));
		}
		return dbo;
	}
}
