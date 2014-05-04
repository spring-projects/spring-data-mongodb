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

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

/**
 * Custom {@link org.springframework.data.mongodb.core.query.Query} implementation to setup a basic query from some arbitrary JSON query string.
 *
 * @author Thomas Risberg
 * @author Oliver Gierke
 * @author Eugene Umputun
 */

public class BasicQuery extends Query {

	private final DBObject queryObject;
	private final DBObject fieldsObject;
	private DBObject sortObject;

	public BasicQuery(String query) {
		this((DBObject) JSON.parse(query));
	}

	public BasicQuery(DBObject queryObject) {
		this(queryObject, null);
	}

	public BasicQuery(String query, String fields) {
		this.queryObject = (DBObject) JSON.parse(query);
		this.fieldsObject = (DBObject) JSON.parse(fields);
	}

	public BasicQuery(DBObject queryObject, DBObject fieldsObject) {
		this.queryObject = queryObject;
		this.fieldsObject = fieldsObject;
	}

	@Override
	public Query addCriteria(Criteria criteria) {
		this.queryObject.putAll(criteria.getCriteriaObject());
		return this;
	}

	@Override
	public DBObject getQueryObject() {
		return this.queryObject;
	}

	@Override
	public DBObject getFieldsObject() {
		return fieldsObject;
	}

	@Override
	public DBObject getSortObject() {

		BasicDBObject result = new BasicDBObject();
		if (sortObject != null) {
			result.putAll(sortObject);
		}

		DBObject overrides = super.getSortObject();
		if (overrides != null) {
			result.putAll(overrides);
		}

		return result;
	}

	public void setSortObject(DBObject sortObject) {
		this.sortObject = sortObject;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		if (!super.equals(o)) return false;

		BasicQuery that = (BasicQuery) o;

		if (fieldsObject != null ? !fieldsObject.equals(that.fieldsObject) : that.fieldsObject != null) return false;
		if (queryObject != null ? !queryObject.equals(that.queryObject) : that.queryObject != null) return false;
		if (sortObject != null ? !sortObject.equals(that.sortObject) : that.sortObject != null) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = super.hashCode();
		result = 31 * result + (queryObject != null ? queryObject.hashCode() : 0);
		result = 31 * result + (fieldsObject != null ? fieldsObject.hashCode() : 0);
		result = 31 * result + (sortObject != null ? sortObject.hashCode() : 0);
		return result;
	}
}
