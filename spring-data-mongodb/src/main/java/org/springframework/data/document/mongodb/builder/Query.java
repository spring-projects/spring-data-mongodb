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

public class Query implements QueryDefinition {
	
	private LinkedHashMap<String, CriteriaDefinition> criteria = new LinkedHashMap<String, CriteriaDefinition>();
	
	private Field fieldSpec;
	
	private Sort sort;

	private int skip;

	private int limit;
	
	public static Criteria newQuery(String key) {
		return new Query().find(key);
	}

	public Criteria find(String key) {
		Criteria c = new Criteria(this);
		this.criteria.put(key, c);
		return c;
	}

	public Query or(QueryDefinition... queries) {
		this.criteria.put("$or", new OrCriteria(queries));
		return this;
	}

	public Field fields() {
		synchronized (this) {
			if (fieldSpec == null) {
				this.fieldSpec = new Field();
			}
		}
		return this.fieldSpec;
	}
	
	public Query limit(int limit) {
		this.limit = limit;
		return this;
	}
	
	public Sort sort() {
		synchronized (this) {
			if (this.sort == null) {
				this.sort = new Sort();
			}
		}
		return this.sort;
	}
	
	public QueryDefinition build() {
		return this;
	}

	public DBObject getQueryObject() {
		DBObject dbo = new BasicDBObject();
		for (String k : criteria.keySet()) {
			CriteriaDefinition c = criteria.get(k);
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

	public DBObject getSortObject() {
		if (this.sort == null) {
			return null;
		}
		return this.sort.getSortObject();
	}

	public int getSkip() {
		return this.skip;
	}

	public int getLimit() {
		return this.limit;
	}
}
