/*
 * Copyright 2014 the original author or authors.
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

import org.bson.Document;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.hamcrest.core.IsEqual;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.util.StringUtils;

/**
 * A {@link TypeSafeMatcher} that tests whether a given {@link Query} matches a query specification.
 * 
 * @author Christoph Strobl
 * @param <T>
 */
public class IsQuery<T extends Query> extends TypeSafeMatcher<T> {

	protected Document query;
	protected Document sort;
	protected Document fields;

	private long skip;
	private int limit;
	private String hint;

	protected IsQuery() {
		query = new Document();
		sort = new Document();
	}

	public static <T extends BasicQuery> IsQuery<T> isQuery() {
		return new IsQuery<T>();
	}

	public IsQuery<T> limitingTo(int limit) {
		this.limit = limit;
		return this;
	}

	public IsQuery<T> skippig(long skip) {
		this.skip = skip;
		return this;
	}

	public IsQuery<T> providingHint(String hint) {
		this.hint = hint;
		return this;
	}

	public IsQuery<T> includingField(String fieldname) {

		if (fields == null) {
			fields = new Document();
		}
		fields.put(fieldname, 1);

		return this;
	}

	public IsQuery<T> excludingField(String fieldname) {

		if (fields == null) {
			fields = new Document();
		}
		fields.put(fieldname, -1);

		return this;
	}

	public IsQuery<T> sortingBy(String fieldname, Direction direction) {

		sort.put(fieldname, Direction.ASC.equals(direction) ? 1 : -1);

		return this;
	}

	public IsQuery<T> where(Criteria criteria) {

		this.query.putAll(criteria.getCriteriaObject());
		return this;
	}

	@Override
	public void describeTo(Description description) {

		BasicQuery expected = new BasicQuery(this.query, this.fields);
		expected.setSortObject(sort);
		expected.skip(this.skip);
		expected.limit(this.limit);

		if (StringUtils.hasText(this.hint)) {
			expected.withHint(this.hint);
		}

		description.appendValue(expected);
	}

	@Override
	protected boolean matchesSafely(T item) {

		if (item == null) {
			return false;
		}

		if (!new IsEqual<Document>(query).matches(item.getQueryObject())) {
			return false;
		}

		if ((item.getSortObject() == null || item.getSortObject().isEmpty()) && !sort.isEmpty()) {
			if (!new IsEqual<Document>(sort).matches(item.getSortObject())) {
				return false;
			}
		}

		if (!new IsEqual<Document>(fields).matches(item.getFieldsObject())) {
			return false;
		}

		if (!new IsEqual<String>(this.hint).matches(item.getHint())) {
			return false;
		}

		if (!new IsEqual(this.skip).matches(item.getSkip())) {
			return false;
		}

		if (!new IsEqual<Integer>(this.limit).matches(item.getLimit())) {
			return false;
		}

		return true;
	}

}
