/*
 * Copyright 2010-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.bson.Document;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Order;
import org.springframework.data.mongodb.InvalidMongoDbApiUsageException;
import org.springframework.data.mongodb.util.BsonUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * MongoDB Query object representing criteria, projection, sorting and query hints.
 *
 * @author Thomas Risberg
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Anton Barkan
 */
public class Query {

	private static final String RESTRICTED_TYPES_KEY = "_$RESTRICTED_TYPES";

	private Set<Class<?>> restrictedTypes = Collections.emptySet();
	private final Map<String, CriteriaDefinition> criteria = new LinkedHashMap<>();
	private @Nullable Field fieldSpec = null;
	private Sort sort = Sort.unsorted();
	private long skip;
	private int limit;
	private @Nullable String hint;

	private Meta meta = new Meta();

	private Optional<Collation> collation = Optional.empty();

	/**
	 * Static factory method to create a {@link Query} using the provided {@link CriteriaDefinition}.
	 *
	 * @param criteriaDefinition must not be {@literal null}.
	 * @return new instance of {@link Query}.
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
	 * @return this.
	 * @since 1.6
	 */
	public Query addCriteria(CriteriaDefinition criteriaDefinition) {

		Assert.notNull(criteriaDefinition, "CriteriaDefinition must not be null");

		CriteriaDefinition existing = this.criteria.get(criteriaDefinition.getKey());
		String key = criteriaDefinition.getKey();

		if (existing == null) {
			this.criteria.put(key, criteriaDefinition);
		} else {
			throw new InvalidMongoDbApiUsageException(
					String.format("Due to limitations of the com.mongodb.BasicDocument, you can't add a second '%s' criteria;"
							+ " Query already contains '%s'", key, serializeToJsonSafely(existing.getCriteriaObject())));
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
	 * Set number of documents to skip before returning results. Use {@literal zero} or a {@literal negative} value to
	 * avoid skipping.
	 *
	 * @param skip number of documents to skip. Use {@literal zero} or a {@literal negative} value to avoid skipping.
	 * @return this.
	 */
	public Query skip(long skip) {
		this.skip = skip;
		return this;
	}

	/**
	 * Limit the number of returned documents to {@code limit}. A {@literal zero} or {@literal negative} value is
	 * considered as unlimited.
	 *
	 * @param limit number of documents to return. Use {@literal zero} or {@literal negative} for unlimited.
	 * @return this.
	 */
	public Query limit(int limit) {
		this.limit = limit;
		return this;
	}

	/**
	 * Configures the query to use the given hint when being executed. The {@code hint} can either be an index name or a
	 * json {@link Document} representation.
	 *
	 * @param hint must not be {@literal null} or empty.
	 * @return this.
	 * @see Document#parse(String)
	 */
	public Query withHint(String hint) {

		Assert.hasText(hint, "Hint must not be empty or null");
		this.hint = hint;
		return this;
	}

	/**
	 * Configures the query to use the given {@link Document hint} when being executed.
	 *
	 * @param hint must not be {@literal null}.
	 * @return this.
	 * @since 2.2
	 */
	public Query withHint(Document hint) {

		Assert.notNull(hint, "Hint must not be null");
		this.hint = hint.toJson();
		return this;
	}

	/**
	 * Sets the given pagination information on the {@link Query} instance. Will transparently set {@code skip} and
	 * {@code limit} as well as applying the {@link Sort} instance defined with the {@link Pageable}.
	 *
	 * @param pageable must not be {@literal null}.
	 * @return this.
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
	 * @param sort must not be {@literal null}.
	 * @return this.
	 */
	public Query with(Sort sort) {

		Assert.notNull(sort, "Sort must not be null");

		if (sort.isUnsorted()) {
			return this;
		}

		sort.stream().filter(Order::isIgnoreCase).findFirst().ifPresent(it -> {

			throw new IllegalArgumentException(String.format("Given sort contained an Order for %s with ignore case;"
					+ " MongoDB does not support sorting ignoring case currently", it.getProperty()));
		});

		this.sort = this.sort.and(sort);

		return this;
	}

	/**
	 * @return the restrictedTypes
	 */
	public Set<Class<?>> getRestrictedTypes() {
		return restrictedTypes;
	}

	/**
	 * Restricts the query to only return documents instances that are exactly of the given types.
	 *
	 * @param type may not be {@literal null}
	 * @param additionalTypes may not be {@literal null}
	 * @return this.
	 */
	public Query restrict(Class<?> type, Class<?>... additionalTypes) {

		Assert.notNull(type, "Type must not be null");
		Assert.notNull(additionalTypes, "AdditionalTypes must not be null");

		if (restrictedTypes == Collections.EMPTY_SET) {
			restrictedTypes = new HashSet<>(1 + additionalTypes.length);
		}

		restrictedTypes.add(type);

		if (additionalTypes.length > 0) {
			restrictedTypes.addAll(Arrays.asList(additionalTypes));
		}

		return this;
	}

	/**
	 * @return the query {@link Document}.
	 */
	public Document getQueryObject() {

		if (criteria.isEmpty() && restrictedTypes.isEmpty()) {
			return BsonUtils.EMPTY_DOCUMENT;
		}

		if (criteria.size() == 1 && restrictedTypes.isEmpty()) {

			for (CriteriaDefinition definition : criteria.values()) {
				return definition.getCriteriaObject();
			}
		}

		Document document = new Document();

		for (CriteriaDefinition definition : criteria.values()) {
			document.putAll(definition.getCriteriaObject());
		}

		if (!restrictedTypes.isEmpty()) {
			document.put(RESTRICTED_TYPES_KEY, getRestrictedTypes());
		}

		return document;
	}

	/**
	 * @return the field {@link Document}.
	 */
	public Document getFieldsObject() {
		return this.fieldSpec == null ? BsonUtils.EMPTY_DOCUMENT : fieldSpec.getFieldsObject();
	}

	/**
	 * @return the sort {@link Document}.
	 */
	public Document getSortObject() {

		if (this.sort.isUnsorted()) {
			return BsonUtils.EMPTY_DOCUMENT;
		}

		Document document = new Document();

		this.sort.forEach(order -> document.put(order.getProperty(), order.isAscending() ? 1 : -1));

		return document;
	}

	/**
	 * Returns {@literal true} if the {@link Query} has a sort parameter.
	 *
	 * @return {@literal true} if sorted.
	 * @see Sort#isSorted()
	 * @since 2.2
	 */
	public boolean isSorted() {
		return sort.isSorted();
	}

	/**
	 * Get the number of documents to skip. {@literal Zero} or a {@literal negative} value indicates no skip.
	 *
	 * @return number of documents to skip
	 */
	public long getSkip() {
		return this.skip;
	}

	/**
	 * Get the maximum number of documents to be return. {@literal Zero} or a {@literal negative} value indicates no
	 * limit.
	 *
	 * @return number of documents to return.
	 */
	public int getLimit() {
		return this.limit;
	}

	/**
	 * @return can be {@literal null}.
	 */
	@Nullable
	public String getHint() {
		return hint;
	}

	/**
	 * @param maxTimeMsec
	 * @return this.
	 * @see Meta#setMaxTimeMsec(long)
	 * @since 1.6
	 */
	public Query maxTimeMsec(long maxTimeMsec) {

		meta.setMaxTimeMsec(maxTimeMsec);
		return this;
	}

	/**
	 * @param timeout must not be {@literal null}.
	 * @return this.
	 * @see Meta#setMaxTime(Duration)
	 * @since 2.1
	 */
	public Query maxTime(Duration timeout) {

		meta.setMaxTime(timeout);
		return this;
	}

	/**
	 * Add a comment to the query that is propagated to the profile log.
	 *
	 * @param comment must not be {@literal null}.
	 * @return this.
	 * @see Meta#setComment(String)
	 * @since 1.6
	 */
	public Query comment(String comment) {

		meta.setComment(comment);
		return this;
	}

	/**
	 * Enables writing to temporary files for aggregation stages and queries. When set to {@literal true}, aggregation
	 * stages can write data to the {@code _tmp} subdirectory in the {@code dbPath} directory.
	 * <p>
	 * Starting in MongoDB 4.2, the profiler log messages and diagnostic log messages includes a {@code usedDisk}
	 * indicator if any aggregation stage wrote data to temporary files due to memory restrictions.
	 *
	 * @param allowDiskUse
	 * @return this.
	 * @see Meta#setAllowDiskUse(Boolean)
	 * @since 3.2
	 */
	public Query allowDiskUse(boolean allowDiskUse) {

		meta.setAllowDiskUse(allowDiskUse);
		return this;
	}

	/**
	 * Set the number of documents to return in each response batch. <br />
	 * Use {@literal 0 (zero)} for no limit. A <strong>negative limit</strong> closes the cursor after returning a single
	 * batch indicating to the server that the client will not ask for a subsequent one.
	 *
	 * @param batchSize The number of documents to return per batch.
	 * @return this.
	 * @see Meta#setCursorBatchSize(int)
	 * @since 2.1
	 */
	public Query cursorBatchSize(int batchSize) {

		meta.setCursorBatchSize(batchSize);
		return this;
	}

	/**
	 * @return this.
	 * @see org.springframework.data.mongodb.core.query.Meta.CursorOption#NO_TIMEOUT
	 * @since 1.10
	 */
	public Query noCursorTimeout() {

		meta.addFlag(Meta.CursorOption.NO_TIMEOUT);
		return this;
	}

	/**
	 * @return this.
	 * @see org.springframework.data.mongodb.core.query.Meta.CursorOption#EXHAUST
	 * @since 1.10
	 */
	public Query exhaust() {

		meta.addFlag(Meta.CursorOption.EXHAUST);
		return this;
	}

	/**
	 * Allows querying of a replica.
	 *
	 * @return this.
	 * @see org.springframework.data.mongodb.core.query.Meta.CursorOption#SECONDARY_READS
	 * @since 3.0.2
	 */
	public Query allowSecondaryReads() {

		meta.addFlag(Meta.CursorOption.SECONDARY_READS);
		return this;
	}

	/**
	 * @return this.
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

		Assert.notNull(meta, "Query meta might be empty but must not be null");
		this.meta = meta;
	}

	/**
	 * Set the {@link Collation} applying language-specific rules for string comparison.
	 *
	 * @param collation can be {@literal null}.
	 * @return this.
	 * @since 2.0
	 */
	public Query collation(@Nullable Collation collation) {

		this.collation = Optional.ofNullable(collation);
		return this;
	}

	/**
	 * Get the {@link Collation} defining language-specific rules for string comparison.
	 *
	 * @return never {@literal null}.
	 * @since 2.0
	 */
	public Optional<Collation> getCollation() {
		return collation;
	}

	protected List<CriteriaDefinition> getCriteria() {
		return new ArrayList<>(this.criteria.values());
	}

	/**
	 * Create an independent copy of the given {@link Query}. <br />
	 * The resulting {@link Query} will not be {@link Object#equals(Object) binary equal} to the given source but
	 * semantically equal in terms of creating the same result when executed.
	 *
	 * @param source The source {@link Query} to use a reference. Must not be {@literal null}.
	 * @return new {@link Query}.
	 * @since 2.2
	 */
	public static Query of(Query source) {

		Assert.notNull(source, "Source must not be null");

		Document sourceFields = source.getFieldsObject();
		Document sourceSort = source.getSortObject();
		Document sourceQuery = source.getQueryObject();

		Query target = new Query() {

			@Override
			public Document getFieldsObject() {
				return BsonUtils.merge(sourceFields, super.getFieldsObject());
			}

			@Override
			public Document getSortObject() {
				return BsonUtils.merge(sourceSort, super.getSortObject());
			}

			@Override
			public Document getQueryObject() {
				return BsonUtils.merge(sourceQuery, super.getQueryObject());
			}

			@Override
			public boolean isSorted() {
				return source.isSorted() || super.isSorted();
			}
		};

		target.skip = source.getSkip();
		target.limit = source.getLimit();
		target.hint = source.getHint();
		target.collation = source.getCollation();
		target.restrictedTypes = new HashSet<>(source.getRestrictedTypes());

		if (source.getMeta().hasValues()) {
			target.setMeta(new Meta(source.getMeta()));
		}

		return target;
	}

	@Override
	public String toString() {
		return String.format("Query: %s, Fields: %s, Sort: %s", serializeToJsonSafely(getQueryObject()),
				serializeToJsonSafely(getFieldsObject()), serializeToJsonSafely(getSortObject()));
	}

	@Override
	public boolean equals(@Nullable Object obj) {

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
		boolean collationEqual = nullSafeEquals(this.collation.orElse(null), that.collation.orElse(null));

		return criteriaEqual && fieldsEqual && sortEqual && hintEqual && skipEqual && limitEqual && metaEqual
				&& collationEqual;
	}

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
		result += 31 * nullSafeHashCode(collation.orElse(null));

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
