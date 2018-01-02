/*
 * Copyright 2015-2018 the original author or authors.
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
package org.springframework.data.mongodb.test.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.hamcrest.core.IsEqual;
import org.springframework.data.mongodb.core.query.SerializationUtils;
import org.springframework.util.ClassUtils;

/**
 * @author Christoph Strobl
 * @param <T>
 */
public class IsBsonObject<T extends Bson> extends TypeSafeMatcher<T> {

	private List<ExpectedBsonContent> expectations = new ArrayList<>();
	private Integer expectedSize;

	public static <T extends Bson> IsBsonObject<T> isBsonObject() {
		return new IsBsonObject<T>();
	}

	@Override
	protected void describeMismatchSafely(T item, Description mismatchDescription) {
		mismatchDescription.appendText("was ").appendValue(SerializationUtils.serializeToJsonSafely(item));
	}

	@Override
	public void describeTo(Description description) {

		if (expectedSize != null) {
			description.appendText(String.format("Expected to contain %s  fields. ", expectedSize));
		}

		for (ExpectedBsonContent expectation : expectations) {

			if (expectation.not) {
				description.appendText(String.format("Path %s should not be present. ", expectation.path));
			} else if (expectation.value == null) {
				description.appendText(String.format("Expected to find path %s. ", expectation.path));
			} else {
				description.appendText(String.format("Expected to find %s for path %s. ", expectation.value, expectation.path));
			}
		}
	}

	@Override
	protected boolean matchesSafely(T item) {

		if (expectedSize != null && item instanceof Document) {

			Document document = (Document) item;
			if (expectedSize != document.keySet().size()) {
				return false;
			}
		}

		if (expectations.isEmpty()) {
			return true;
		}

		for (ExpectedBsonContent expectation : expectations) {

			Object o = getValue(item, expectation.path);

			if (o == null && expectation.not) {
				return true;
			}

			if (o == null) {
				return false;
			}

			if (expectation.type != null) {

				if (ClassUtils.isAssignable(List.class, expectation.type)
						&& ClassUtils.isAssignable(List.class, o.getClass())) {
					return true;
				}

				return ClassUtils.isAssignable(expectation.type, o.getClass());
			}

			if (expectation.value != null && !new IsEqual<Object>(expectation.value).matches(o)) {
				return false;
			}

			if (o != null && expectation.not) {
				return false;
			}

		}
		return true;
	}

	public IsBsonObject<T> containing(String key) {

		ExpectedBsonContent expected = new ExpectedBsonContent();
		expected.path = key;

		this.expectations.add(expected);
		return this;
	}

	public IsBsonObject<T> containing(String key, Class<?> type) {

		ExpectedBsonContent expected = new ExpectedBsonContent();
		expected.path = key;
		expected.type = type;

		this.expectations.add(expected);
		return this;
	}

	public IsBsonObject<T> containing(String key, Object value) {

		if (value == null) {
			return notContaining(key);
		}

		ExpectedBsonContent expected = new ExpectedBsonContent();
		expected.path = key;
		expected.type = ClassUtils.getUserClass(value);
		expected.value = value;

		this.expectations.add(expected);
		return this;
	}

	public IsBsonObject<T> notContaining(String key) {

		ExpectedBsonContent expected = new ExpectedBsonContent();
		expected.path = key;
		expected.not = true;

		this.expectations.add(expected);
		return this;
	}

	public IsBsonObject<T> withSize(int size) {

		this.expectedSize = Integer.valueOf(size);
		return this;
	}

	static class ExpectedBsonContent {
		String path;
		Class<?> type;
		Object value;
		boolean not = false;
	}

	Object getValue(Bson source, String path) {

		String[] fragments = path.split("(?<!\\\\)\\.");

		if (fragments.length == 1) {
			return ((Document) source).get(path.replace("\\.", "."));
		}

		Iterator<String> it = Arrays.asList(fragments).iterator();

		Object current = source;
		while (it.hasNext()) {

			String key = it.next().replace("\\.", ".");

			if (!(current instanceof Bson) && !key.startsWith("[")) {
				return null;
			}

			if (key.startsWith("[")) {
				String indexNumber = key.substring(1, key.indexOf("]"));
				if (current instanceof List) {
					current = ((List) current).get(Integer.valueOf(indexNumber));
				}
				if (!it.hasNext()) {
					return current;
				}
			} else {

				if (current instanceof Document) {
					current = ((Document) current).get(key);
				}

				if (!it.hasNext()) {
					return current;
				}

			}
		}

		throw new NoSuchElementException(String.format("Unable to find '%s' in %s.", path, source));
	}
}
