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
package org.springframework.datastore.document.mongodb.query;

import java.util.LinkedHashMap;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class QueryBuilder implements Query {
	
	private LinkedHashMap<String, CriteriaSpec> criteria = new LinkedHashMap<String, CriteriaSpec>();

	public Criteria find(String key) {
		Criteria c = new Criteria(this);
		this.criteria.put(key, c);
		return c;
	}

	public QueryBuilder or(Query... queries) {
		this.criteria.put("$or", new OrCriteria(queries));
		return this;
	}

	public FieldSpecification fields() {
		return new FieldSpecification();
	}
	
	public SliceSpecification slice() {
		return new SliceSpecification();
	}
	
	public SortSpecification sort() {
		return new SortSpecification();
	}
	
	public QueryBuilder limit(int limit) {
		return this;
	}
	
	public Query build() {
		return this;
	}

	public DBObject getQueryObject() {
		DBObject dbo = new BasicDBObject();
		for (String k : criteria.keySet()) {
			CriteriaSpec c = criteria.get(k);
			DBObject cl = c.getCriteriaObject(k);
			dbo.putAll(cl);
		}
		return dbo;
	}

}
