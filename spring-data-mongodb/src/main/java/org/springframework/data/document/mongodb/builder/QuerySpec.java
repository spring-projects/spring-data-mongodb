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

import java.util.LinkedHashMap;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class QuerySpec implements Query {
	
	private LinkedHashMap<String, Criteria> criteria = new LinkedHashMap<String, Criteria>();
	
	private FieldSpec fieldSpec;
	
	private int limit;

	public CriteriaSpec find(String key) {
		CriteriaSpec c = new CriteriaSpec(this);
		this.criteria.put(key, c);
		return c;
	}

	public QuerySpec or(Query... queries) {
		this.criteria.put("$or", new OrCriteriaSpec(queries));
		return this;
	}

	public FieldSpec fields() {
		synchronized (this) {
			if (fieldSpec == null) {
				this.fieldSpec = new FieldSpec();
			}
		}
		return this.fieldSpec;
	}
	
	public QuerySpec limit(int limit) {
		this.limit = limit;
		return this;
	}
	
	public Query build() {
		return this;
	}

	public DBObject getQueryObject() {
		DBObject dbo = new BasicDBObject();
		for (String k : criteria.keySet()) {
			Criteria c = criteria.get(k);
			DBObject cl = c.getCriteriaObject(k);
			dbo.putAll(cl);
		}
		return dbo;
	}

	public DBObject getFieldsObject() {
		if (this.fieldSpec == null) {
			return null;
		}
		return fieldSpec.getFieldsObject();
	}

	public int getLimit() {
		return this.limit;
	}

}
