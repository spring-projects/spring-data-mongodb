/*
 * Copyright 2010-2012 the original author or authors.
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

import static org.springframework.data.mongodb.core.query.SerializationUtils.*;
import static org.springframework.util.ObjectUtils.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Order;
import org.springframework.data.mongodb.InvalidMongoDbApiUsageException;
import org.springframework.util.Assert;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * @author Thomas Risberg
 * @author Oliver Gierke
 */
public class Query {

	private LinkedHashMap<String, Criteria> criteria = new LinkedHashMap<String, Criteria>();
	private Field fieldSpec;
	private Sort coreSort;
	@SuppressWarnings("deprecation")
	private org.springframework.data.mongodb.core.query.Sort sort;
	private int skip;
	private int limit;
	private String hint;

	/**
	 * Static factory method to create a Query using the provided criteria
	 * 
	 * @param criteria
	 * @return
	 */
	public static Query query(Criteria criteria) {
		return new Query(criteria);
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
		} else {
			throw new InvalidMongoDbApiUsageException("Due to limitations of the com.mongodb.BasicDBObject, "
					+ "you can't add a second '" + key + "' criteria. " + "Query already contains '"
					+ existing.getCriteriaObject() + "'.");
		}
		return this;
	}

	public Field fields() {
		if (fieldSpec == null) {
			this.fieldSpec = new Field();
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

	/**
	 * Configures the query to use the given hint when being executed.
	 * 
	 * @param name must not be {@literal null} or empty.
	 * @return
	 */
	public Query withHint(String name) {
		Assert.hasText(name, "Hint must not be empty or null!");
		this.hint = name;
		return this;
	}

	/**
	 * Returns a {@link org.springframework.data.mongodb.core.query.Sort} instance to define ordering properties.
	 * 
	 * @deprecated use {@link #with(Sort)} instead
	 * @return
	 */
	@Deprecated
	public org.springframework.data.mongodb.core.query.Sort sort() {
		if (this.sort == null) {
			this.sort = new org.springframework.data.mongodb.core.query.Sort();
		}

		return this.sort;
	}

	/**
	 * Sets the given pagination information on the {@link Query} instance. Will transparently set {@code skip} and
	 * {@code limit} as well as applying the {@link Sort} instance defined with the {@link Pageable}.
	 * 
	 * @param pageable
	 * @return
	 */
	public Query with(Pageable pageable) {

		if (pageable == null) {
			return this;
		}

		this.limit = pageable.getPageSize();
		this.skip = pageable.getOffset();

		return with(pageable.getSort());
	}

	/**
	 * Adds a {@link Sort} to the {@link Query} instance.
	 * 
	 * @param sort
	 * @return
	 */
	public Query with(Sort sort) {

		if (sort == null) {
			return this;
		}

		for (Order order : sort) {
			if (order.isIgnoreCase()) {
				throw new IllegalArgumentException(String.format("Gven sort contained an Order for %s with ignore case! "
						+ "MongoDB does not support sorting ignoreing case currently!", order.getProperty()));
			}
		}

		if (this.coreSort == null) {
			this.coreSort = sort;
		} else {
			this.coreSort = this.coreSort.and(sort);
		}

		return this;
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

	@SuppressWarnings("deprecation")
	public DBObject getSortObject() {

		if (this.coreSort == null && this.sort == null) {
			return null;
		}

		DBObject dbo = new BasicDBObject();

		if (this.coreSort != null) {
			for (org.springframework.data.domain.Sort.Order order : this.coreSort) {
				dbo.put(order.getProperty(), order.isAscending() ? 1 : -1);
			}
		}

		if (this.sort != null) {
			dbo.putAll(this.sort.getSortObject());
		}

		return dbo;
	}

	public int getSkip() {
		return this.skip;
	}

	public int getLimit() {
		return this.limit;
	}

	public String getHint() {
		return hint;
	}

	protected List<Criteria> getCriteria() {
		return new ArrayList<Criteria>(this.criteria.values());
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format("Query: %s, Fields: %s, Sort: %s", serializeToJsonSafely(getQueryObject()),
				serializeToJsonSafely(getFieldsObject()), serializeToJsonSafely(getSortObject()));
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {

		if (this == obj) {
			return true;
		}

		if (obj == null || !getClass().equals(obj.getClass())) {
			return false;
		}

		Query that = (Query) obj;

		boolean criteriaEqual = this.criteria.equals(that.criteria);
		boolean fieldsEqual = this.fieldSpec == null ? that.fieldSpec == null : this.fieldSpec.equals(that.fieldSpec);
		boolean sortEqual = this.sort == null ? that.sort == null : this.sort.equals(that.sort);
		boolean hintEqual = this.hint == null ? that.hint == null : this.hint.equals(that.hint);
		boolean skipEqual = this.skip == that.skip;
		boolean limitEqual = this.limit == that.limit;

		return criteriaEqual && fieldsEqual && sortEqual && hintEqual && skipEqual && limitEqual;
	}

	/* 
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {

		int result = 17;

		result += 31 * criteria.hashCode();
		result += 31 * nullSafeHashCode(fieldSpec);
		result += 31 * nullSafeHashCode(sort);
		result += 31 * nullSafeHashCode(hint);
		result += 31 * skip;
		result += 31 * limit;

		return result;
	}
}
