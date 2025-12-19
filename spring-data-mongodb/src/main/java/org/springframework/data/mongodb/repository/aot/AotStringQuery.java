/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.mongodb.repository.aot;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.bson.Document;
import org.jspecify.annotations.Nullable;
import org.springframework.data.domain.KeysetScrollPosition;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Field;
import org.springframework.data.mongodb.core.query.Meta;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.aot.AotPlaceholders.AsListPlaceholder;
import org.springframework.data.mongodb.repository.aot.AotPlaceholders.RegexPlaceholder;
import org.springframework.util.StringUtils;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;

/**
 * Helper to capture setting for AOT queries.
 *
 * @author Christoph Strobl
 * @since 5.0
 */
class AotStringQuery extends Query {

	private final Query delegate;
	private @Nullable String raw;
	private @Nullable String sort;
	private @Nullable String fields;

	private List<Object> placeholders = new ArrayList<>();

	public AotStringQuery(Query query, List<Object> placeholders) {
		this.delegate = query;
		this.placeholders = placeholders;
	}

	public AotStringQuery(String query) {
		this.delegate = new Query();
		this.raw = query;
	}

	@Nullable
	String getQueryString() {

		if (StringUtils.hasText(raw)) {
			return raw;
		}

		Document queryObj = getQueryObject();
		if (queryObj.isEmpty()) {
			return null;
		}
		return toJson(queryObj);
	}

	public Query sort(String sort) {
		this.sort = sort;
		return this;
	}

	boolean isRegexPlaceholderAt(int index) {
		if (this.placeholders.isEmpty()) {
			return false;
		}

		return obtainAndPotentiallyUnwrapPlaceholder(index) instanceof RegexPlaceholder;
	}

	@Nullable
	String getRegexOptions(int index) {
		if (this.placeholders.isEmpty()) {
			return null;
		}

		Object placeholderValue = obtainAndPotentiallyUnwrapPlaceholder(index);
		return placeholderValue instanceof RegexPlaceholder rgp ? rgp.regexOptions() : null;
	}

	@Nullable Object obtainAndPotentiallyUnwrapPlaceholder(int index) {

		if (this.placeholders.isEmpty()) {
			return null;
		}

		Object placeholerValue = this.placeholders.get(index);
		if (placeholerValue instanceof AsListPlaceholder asListPlaceholder) {
			placeholerValue = asListPlaceholder.placeholder();
		}
		return placeholerValue;
	}

	@Override
	public Field fields() {
		return delegate.fields();
	}

	@Override
	public boolean hasReadConcern() {
		return delegate.hasReadConcern();
	}

	@Override
	public @Nullable ReadConcern getReadConcern() {
		return delegate.getReadConcern();
	}

	@Override
	public boolean hasReadPreference() {
		return delegate.hasReadPreference();
	}

	@Override
	public @Nullable ReadPreference getReadPreference() {
		return delegate.getReadPreference();
	}

	@Override
	public boolean hasKeyset() {
		return delegate.hasKeyset();
	}

	@Override
	public @Nullable KeysetScrollPosition getKeyset() {
		return delegate.getKeyset();
	}

	@Override
	public Set<Class<?>> getRestrictedTypes() {
		return delegate.getRestrictedTypes();
	}

	@Override
	public Document getQueryObject() {
		return delegate.getQueryObject();
	}

	@Override
	public Document getFieldsObject() {
		return delegate.getFieldsObject();
	}

	@Override
	public Document getSortObject() {
		return delegate.getSortObject();
	}

	@Override
	public boolean isSorted() {
		return delegate.isSorted() || StringUtils.hasText(sort);
	}

	@Override
	public long getSkip() {
		return delegate.getSkip();
	}

	@Override
	public boolean isLimited() {
		return delegate.isLimited();
	}

	@Override
	public int getLimit() {
		return delegate.getLimit();
	}

	@Override
	public @Nullable String getHint() {
		return delegate.getHint();
	}

	@Override
	public Meta getMeta() {
		return delegate.getMeta();
	}

	@Override
	public Optional<Collation> getCollation() {
		return delegate.getCollation();
	}

	@Nullable
	String getSortString() {
		if (StringUtils.hasText(sort)) {
			return sort;
		}
		Document sort = getSortObject();
		if (sort.isEmpty()) {
			return null;
		}
		return toJson(sort);
	}

	@Nullable
	String getFieldsString() {
		if (StringUtils.hasText(fields)) {
			return fields;
		}

		Document fields = getFieldsObject();
		if (fields.isEmpty()) {
			return null;
		}
		return toJson(fields);
	}

	AotStringQuery fields(String fields) {
		this.fields = fields;
		return this;
	}

	String toJson(Document source) {
		return DocumentSerializer.toJson(source);
	}
}
