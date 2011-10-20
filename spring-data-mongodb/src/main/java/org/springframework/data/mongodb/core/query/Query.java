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
package org.springframework.data.mongodb.core.query;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.springframework.data.mongodb.InvalidMongoDbApiUsageException;

public class Query {

	private LinkedHashMap<String, Criteria> criteria = new LinkedHashMap<String, Criteria>();
	private Field fieldSpec;
	private Sort sort;
	private int skip;
	private int limit;

	/**
	 * Static factory method to create a Query using the provided criteria
	 *
	 * @param critera
	 * @return
	 */
	public static Query query(Criteria critera) {
		return new Query(critera);
	}

	public Query() {
	}

	public Query(Criteria criteria) {
		addCriteria(criteria);
	}

	public Query addCriteria(Criteria criteria) {
		CriteriaDefinition existing = this.criteria.get(criteria.getKey());
		String key = criteria.getKey();
		if (existing == null) {
			this.criteria.put(key, criteria);
		}
		else {
			throw new InvalidMongoDbApiUsageException("Due to limitations of the com.mongodb.BasicDBObject, " +
					"you can't add a second '" + key + "' criteria. " +
					"Query already contains '" + existing.getCriteriaObject() + "'.");
		}
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

	public Query skip(int skip) {
		this.skip = skip;
		return this;
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

	public DBObject getQueryObject() {
		DBObject dbo = new BasicDBObject();
		for (String k : criteria.keySet()) {
			CriteriaDefinition c = criteria.get(k);
			DBObject cl = c.getCriteriaObject();
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

	protected List<Criteria> getCriteria() {
		return new ArrayList<Criteria>(this.criteria.values());
	}
}
