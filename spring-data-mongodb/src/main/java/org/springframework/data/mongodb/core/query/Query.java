/*
 * Copyright 2010-2014 the original author or authors.
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
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
 * @author Thomas Darimont
 */
public class Query {

	private static final String RESTRICTED_TYPES_KEY = "_$RESTRICTED_TYPES";

	private final Set<Class<?>> restrictedTypes = new HashSet<Class<?>>();
	private final Map<String, Criteria> criteria = new LinkedHashMap<String, Criteria>();
	private Field fieldSpec;
	private Sort sort;
	private int skip;
	private int limit;
	private String hint;

	/**
	 * Static factory method to create a {@link Query} using the provided {@link Criteria}.
	 * 
	 * @param criteria must not be {@literal null}.
	 * @return
	 */
	public static Query query(Criteria criteria) {
		return new Query(criteria);
	}

	public Query() {}

	/**
	 * Creates a new {@link Query} using the given {@link Criteria}.
	 * 
	 * @param criteria must not be {@literal null}.
	 */
	public Query(Criteria criteria) {
		addCriteria(criteria);
	}

	/**
	 * Adds the given {@link Criteria} to the current {@link Query}.
	 * 
	 * @param criteria must not be {@literal null}.
	 * @return
	 */
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

	/**
	 * Set number of documents to skip before returning results.
	 * 
	 * @param skip
	 * @return
	 */
	public Query skip(int skip) {
		this.skip = skip;
		return this;
	}

	/**
	 * Limit the number of returned documents to {@code limit}.
	 * 
	 * @param limit
	 * @return
	 */
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

		if (this.sort == null) {
			this.sort = sort;
		} else {
			this.sort = this.sort.and(sort);
		}

		return this;
	}

	/**
	 * @return the restrictedTypes
	 */
	public Set<Class<?>> getRestrictedTypes() {
		return restrictedTypes == null ? Collections.<Class<?>> emptySet() : restrictedTypes;
	}

	/**
	 * Restricts the query to only return documents instances that are exactly of the given types.
	 * 
	 * @param type may not be {@literal null}
	 * @param additionalTypes may not be {@literal null}
	 * @return
	 */
	public Query restrict(Class<?> type, Class<?>... additionalTypes) {

		Assert.notNull(type, "Type must not be null!");
		Assert.notNull(additionalTypes, "AdditionalTypes must not be null");

		restrictedTypes.add(type);
		for (Class<?> additionalType : additionalTypes) {
			restrictedTypes.add(additionalType);
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

		if (!restrictedTypes.isEmpty()) {
			dbo.put(RESTRICTED_TYPES_KEY, getRestrictedTypes());
		}

		return dbo;
	}

	public DBObject getFieldsObject() {
		return this.fieldSpec == null ? null : fieldSpec.getFieldsObject();
	}

	public DBObject getSortObject() {

		if (this.sort == null) {
			return null;
		}

		DBObject dbo = new BasicDBObject();

		for (org.springframework.data.domain.Sort.Order order : this.sort) {
			dbo.put(order.getProperty(), order.isAscending() ? 1 : -1);
		}

		return dbo;
	}

	/**
	 * Get the number of documents to skip.
	 * 
	 * @return
	 */
	public int getSkip() {
		return this.skip;
	}

	/**
	 * Get the maximum number of documents to be return.
	 * 
	 * @return
	 */
	public int getLimit() {
		return this.limit;
	}

	/**
	 * @return
	 */
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

	/**
	 * Returns whether the given key is the one used to hold the type restriction information.
	 * 
	 * @deprecated don't call this method as the restricted type handling will undergo some significant changes going
	 *             forward.
	 * @param key
	 * @return
	 */
	@Deprecated
	public static boolean isRestrictedTypeKey(String key) {
		return RESTRICTED_TYPES_KEY.equals(key);
	}
}
