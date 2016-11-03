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

import static org.springframework.util.ObjectUtils.*;

import org.bson.Document;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

/**
 * Custom {@link Query} implementation to setup a basic query from some arbitrary JSON query string.
 * 
 * @author Thomas Risberg
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author John Willemin
 * @author Mark Paluch
 */
public class BasicQuery extends Query {

	private final Document queryObject;
	private Document fieldsObject;
	private Document sortObject;

	/**
	 * Create a new {@link BasicQuery} given a JSON {@code query}.
	 *
	 * @param query may be {@literal null}.
	 */
	public BasicQuery(String query) {
		this(query, null);
	}

	/**
	 * Create a new {@link BasicQuery} given a query {@link Document}.
	 *
	 * @param queryObject may be {@literal null}.
	 */
	public BasicQuery(Document queryObject) {
		this(queryObject, null);
	}

	/**
	 * Create a new {@link BasicQuery} given a JSON {@code query} and {@code fields}.
	 *
	 * @param query may be {@literal null}.
	 * @param fields may be {@literal null}.
	 */
	public BasicQuery(String query, String fields) {
		this.queryObject = query != null ? new Document(((DBObject) JSON.parse(query)).toMap()) : null;
		this.fieldsObject = fields != null ? new Document(((DBObject) JSON.parse(fields)).toMap()) : null;
	}

	/**
	 * Create a new {@link BasicQuery} given a query {@link Document} and field specification {@link Document}.
	 *
	 * @param queryObject may be {@literal null}.
	 * @param fieldsObject may be {@literal null}.
	 */
	public BasicQuery(Document queryObject, Document fieldsObject) {
		this.queryObject = queryObject;
		this.fieldsObject = fieldsObject;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.query.Query#addCriteria(org.springframework.data.mongodb.core.query.CriteriaDefinition)
	 */
	@Override
	public Query addCriteria(CriteriaDefinition criteria) {
		this.queryObject.putAll(criteria.getCriteriaObject());
		return this;
	}

	@Override
	public Document getQueryObject() {
		return this.queryObject;
	}

	@Override
	public Document getFieldsObject() {

		if (fieldsObject == null) {
			return super.getFieldsObject();
		}

		if (super.getFieldsObject() != null) {

			Document combinedFieldsObject = new Document();
			combinedFieldsObject.putAll(fieldsObject);
			combinedFieldsObject.putAll(super.getFieldsObject());
			return combinedFieldsObject;
		}

		return fieldsObject;
	}

	@Override
	public Document getSortObject() {

		Document result = new Document();
		if (sortObject != null) {
			result.putAll(sortObject);
		}

		Document overrides = super.getSortObject();
		if (overrides != null) {
			result.putAll(overrides);
		}

		return result;
	}

	public void setSortObject(Document sortObject) {
		this.sortObject = sortObject;
	}

	/**
	 * @since 1.6
	 * @param fieldsObject
	 */
	protected void setFieldsObject(Document fieldsObject) {
		this.fieldsObject = fieldsObject;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.query.Query#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object o) {

		if (this == o) {
			return true;
		}

		if (!(o instanceof BasicQuery)) {
			return false;
		}

		BasicQuery that = (BasicQuery) o;

		return querySettingsEquals(that) && //
				nullSafeEquals(fieldsObject, that.fieldsObject) && //
				nullSafeEquals(queryObject, that.queryObject) && //
				nullSafeEquals(sortObject, that.sortObject);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.query.Query#hashCode()
	 */
	@Override
	public int hashCode() {

		int result = super.hashCode();
		result = 31 * result + nullSafeHashCode(queryObject);
		result = 31 * result + nullSafeHashCode(fieldsObject);
		result = 31 * result + nullSafeHashCode(sortObject);

		return result;
	}
}
