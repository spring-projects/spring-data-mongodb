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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import org.springframework.data.document.InvalidDocumentStoreApiUsageException;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class CriteriaSpec implements Criteria {
	
	private QuerySpec qb = null;
	
	private LinkedHashMap<String, Object> criteria = new LinkedHashMap<String, Object>();

	private Object isValue = null;
	
	
	public CriteriaSpec(QuerySpec qb) {
		super();
		this.qb = qb;
	}


	public CriteriaSpec and(String key) {
		return qb.find(key);
	}


	public CriteriaSpec is(Object o) {
		if (isValue != null) {
			throw new InvalidDocumentStoreApiUsageException("Multiple 'is' values declared.");
		}
		this.isValue = o;
		return this;
	}

	public CriteriaSpec lt(Object o) {
		criteria.put("$lt", o);
		return this;
	}
	
	public CriteriaSpec lte(Object o) {
		criteria.put("$lte", o);
		return this;
	}
	
	public CriteriaSpec gt(Object o) {
		criteria.put("$gt", o);
		return this;
	}
	
	public CriteriaSpec gte(Object o) {
		criteria.put("$gte", o);
		return this;
	}
	
	public CriteriaSpec in(Object... o) {
		criteria.put("$in", o);
		return this;
	}

	public CriteriaSpec nin(Object... o) {
		criteria.put("$min", o);
		return this;
	}

	public CriteriaSpec mod(Number value, Number remainder) {
		List<Object> l = new ArrayList<Object>();
		l.add(value);
		l.add(remainder);
		criteria.put("$mod", l);
		return this;
	}

	public CriteriaSpec all(Object o) {
		criteria.put("$is", o);
		return this;
	}

	public CriteriaSpec size(Object o) {
		criteria.put("$is", o);
		return this;
	}

	public CriteriaSpec exists(boolean b) {
		return this;
	}

	public CriteriaSpec type(int t) {
		return this;
	}

	public CriteriaSpec not() {
		criteria.put("$not", null);
		return this;
	}
	
	public CriteriaSpec regExp(String re) {
		return this;
	}

	public void or(List<Query> queries) {
		criteria.put("$or", queries);		
	}
	
	public Query build() {
		return qb.build(); 
	}

	/* (non-Javadoc)
	 * @see org.springframework.datastore.document.mongodb.query.Criteria#getCriteriaObject(java.lang.String)
	 */
	public DBObject getCriteriaObject(String key) {
		DBObject dbo = new BasicDBObject();
		boolean not = false;
		for (String k : criteria.keySet()) {
			if (not) {
				DBObject notDbo = new BasicDBObject();
				notDbo.put(k, criteria.get(k));
				dbo.put("$not", notDbo);
				not = false;
			}
			else {
				if ("$not".equals(k)) {
					not = true;
				}
				else {
					dbo.put(k, criteria.get(k));
				}
			}
		}
		DBObject queryCriteria = new BasicDBObject();
		if (isValue != null) {
			queryCriteria.put(key, isValue);
			queryCriteria.putAll(dbo);
		}
		else {
			queryCriteria.put(key, dbo);
		}
		return queryCriteria;
	}

}
