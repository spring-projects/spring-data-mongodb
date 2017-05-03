/*
 * Copyright 2010-2017 the original author or authors.
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
import java.util.concurrent.TimeUnit;

import org.bson.Document;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Order;
import org.springframework.data.mongodb.InvalidMongoDbApiUsageException;
import org.springframework.util.Assert;

/**
 * @author Thomas Risberg
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class Query {

	private static final String RESTRICTED_TYPES_KEY = "_$RESTRICTED_TYPES";

	private final Set<Class<?>> restrictedTypes = new HashSet<Class<?>>();
	private final Map<String, CriteriaDefinition> criteria = new LinkedHashMap<String, CriteriaDefinition>();
	private Field fieldSpec = null;
	private Sort sort = Sort.unsorted();
	private long skip;
	private int limit;
	private String hint;

	private Meta meta = new Meta();

	/**
	 * Static factory method to create a {@link Query} using the provided {@link CriteriaDefinition}.
	 *
	 * @param criteriaDefinition must not be {@literal null}.
	 * @return
	 * @since 1.6
	 */
	public static Query query(CriteriaDefinition criteriaDefinition) {
		return new Query(criteriaDefinition);
	}

	public Query() {}

	/**
	 * Creates a new {@link Query} using the given {@link CriteriaDefinition}.
	 *
	 * @param criteriaDefinition must not be {@literal null}.
	 * @since 1.6
	 */
	public Query(CriteriaDefinition criteriaDefinition) {
		addCriteria(criteriaDefinition);
	}

	/**
	 * Adds the given {@link CriteriaDefinition} to the current {@link Query}.
	 *
	 * @param criteriaDefinition must not be {@literal null}.
	 * @return
	 * @since 1.6
	 */
	public Query addCriteria(CriteriaDefinition criteriaDefinition) {

		CriteriaDefinition existing = this.criteria.get(criteriaDefinition.getKey());
		String key = criteriaDefinition.getKey();

		if (existing == null) {
			this.criteria.put(key, criteriaDefinition);
		} else {
			throw new InvalidMongoDbApiUsageException(
					String.format("Due to limitations of the com.mongodb.BasicDocument, you can't add a second '%s' criteria. "
							+ "Query already contains '%s'", key, serializeToJsonSafely(existing.getCriteriaObject())));
		}

		return this;
	}

	public Field fields() {

		if (this.fieldSpec == null) {
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
	public Query skip(long skip) {
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

		if (pageable.isUnpaged()) {
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

		Assert.notNull(sort, "Sort must not be null!");

		if (sort.isUnsorted()) {
			return this;
		}

		sort.stream().filter(Order::isIgnoreCase).findFirst().ifPresent(it -> {

			throw new IllegalArgumentException(String.format("Given sort contained an Order for %s with ignore case! "
					+ "MongoDB does not support sorting ignoring case currently!", it.getProperty()));
		});

		this.sort = this.sort.and(sort);

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

	public Document getQueryObject() {

		Document document = new Document();

		for (CriteriaDefinition definition : criteria.values()) {
			document.putAll(definition.getCriteriaObject());
		}

		if (!restrictedTypes.isEmpty()) {
			document.put(RESTRICTED_TYPES_KEY, getRestrictedTypes());
		}

		return document;
	}

	public Document getFieldsObject() {
		return this.fieldSpec == null ? null : fieldSpec.getFieldsObject();
	}

	public Document getSortObject() {

		if (this.sort.isUnsorted()) {
			return null;
		}

		Document document = new Document();

		this.sort.stream()//
				.forEach(order -> document.put(order.getProperty(), order.isAscending() ? 1 : -1));

		return document;
	}

	/**
	 * Get the number of documents to skip.
	 *
	 * @return
	 */
	public long getSkip() {
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

	/**
	 * @param maxTimeMsec
	 * @return
	 * @see Meta#setMaxTimeMsec(long)
	 * @since 1.6
	 */
	public Query maxTimeMsec(long maxTimeMsec) {

		meta.setMaxTimeMsec(maxTimeMsec);
		return this;
	}

	/**
	 * @param timeout
	 * @param timeUnit
	 * @return
	 * @see Meta#setMaxTime(long, TimeUnit)
	 * @since 1.6
	 */
	public Query maxTime(long timeout, TimeUnit timeUnit) {

		meta.setMaxTime(timeout, timeUnit);
		return this;
	}

	/**
	 * @param maxScan
	 * @return
	 * @see Meta#setMaxScan(long)
	 * @since 1.6
	 */
	public Query maxScan(long maxScan) {

		meta.setMaxScan(maxScan);
		return this;
	}

	/**
	 * @param comment
	 * @return
	 * @see Meta#setComment(String)
	 * @since 1.6
	 */
	public Query comment(String comment) {

		meta.setComment(comment);
		return this;
	}

	/**
	 * @return
	 * @see Meta#setSnapshot(boolean)
	 * @since 1.6
	 */
	public Query useSnapshot() {

		meta.setSnapshot(true);
		return this;
	}

	/**
	 * @return
	 * @see org.springframework.data.mongodb.core.query.Meta.CursorOption#NO_TIMEOUT
	 * @since 1.10
	 */
	public Query noCursorTimeout() {

		meta.addFlag(Meta.CursorOption.NO_TIMEOUT);
		return this;
	}

	/**
	 * @return
	 * @see org.springframework.data.mongodb.core.query.Meta.CursorOption#EXHAUST
	 * @since 1.10
	 */
	public Query exhaust() {

		meta.addFlag(Meta.CursorOption.EXHAUST);
		return this;
	}

	/**
	 * @return
	 * @see org.springframework.data.mongodb.core.query.Meta.CursorOption#SLAVE_OK
	 * @since 1.10
	 */
	public Query slaveOk() {

		meta.addFlag(Meta.CursorOption.SLAVE_OK);
		return this;
	}

	/**
	 * @return
	 * @see org.springframework.data.mongodb.core.query.Meta.CursorOption#PARTIAL
	 * @since 1.10
	 */
	public Query partialResults() {

		meta.addFlag(Meta.CursorOption.PARTIAL);
		return this;
	}

	/**
	 * @return never {@literal null}.
	 * @since 1.6
	 */
	public Meta getMeta() {
		return meta;
	}

	/**
	 * @param meta must not be {@literal null}.
	 * @since 1.6
	 */
	public void setMeta(Meta meta) {

		Assert.notNull(meta, "Query meta might be empty but must not be null.");
		this.meta = meta;
	}

	protected List<CriteriaDefinition> getCriteria() {
		return new ArrayList<CriteriaDefinition>(this.criteria.values());
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

		return querySettingsEquals((Query) obj);
	}

	/**
	 * Tests whether the settings of the given {@link Query} are equal to this query.
	 *
	 * @param that
	 * @return
	 */
	protected boolean querySettingsEquals(Query that) {

		boolean criteriaEqual = this.criteria.equals(that.criteria);
		boolean fieldsEqual = nullSafeEquals(this.fieldSpec, that.fieldSpec);
		boolean sortEqual = this.sort.equals(that.sort);
		boolean hintEqual = nullSafeEquals(this.hint, that.hint);
		boolean skipEqual = this.skip == that.skip;
		boolean limitEqual = this.limit == that.limit;
		boolean metaEqual = nullSafeEquals(this.meta, that.meta);

		return criteriaEqual && fieldsEqual && sortEqual && hintEqual && skipEqual && limitEqual && metaEqual;
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
		result += 31 * nullSafeHashCode(meta);

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
