/*
 * Copyright 2017-2018 the original author or authors.
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

import static org.assertj.core.error.ElementsShouldBe.*;
import static org.assertj.core.error.ShouldContain.*;
import static org.assertj.core.error.ShouldContainKeys.*;
import static org.assertj.core.error.ShouldNotContain.*;
import static org.assertj.core.error.ShouldNotContainKeys.*;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Consumer;

import org.assertj.core.api.AbstractMapAssert;
import org.assertj.core.api.Condition;
import org.assertj.core.error.ShouldContainAnyOf;
import org.assertj.core.internal.Failures;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Assertions for Mongo's {@link Document}. Assertions based on keys/entries are translated to document paths allowing
 * to assert nested elements.
 *
 * <pre>
 * <code>
 *  Document document = Document.parse("{ $set: { concreteInnerList: [ { foo: "bar", _class: … }] } }");
 *
 *  assertThat(mappedUpdate).containsKey("$set.concreteInnerList.[0].foo").doesNotContainKey("$set.concreteInnerList.[0].bar");
 * </code>
 * </pre>
 *
 * @author Mark Paluch
 */
public class DocumentAssert extends AbstractMapAssert<DocumentAssert, Map<String, Object>, String, Object> {

	private final Document actual;

	DocumentAssert(Document actual) {

		super(actual, DocumentAssert.class);
		this.actual = actual;
	}

	/*
	 * (non-Javadoc)
	 * @see org.assertj.core.api.AbstractMapAssert#containsEntry(java.lang.Object, java.lang.Object)
	 */
	@Override
	public DocumentAssert containsEntry(String key, Object value) {

		Assert.hasText(key, "The key to look for must not be empty!");

		Lookup<?> lookup = lookup(key);

		if (!lookup.isPathFound() || !ObjectUtils.nullSafeEquals(value, lookup.getValue())) {
			throw Failures.instance().failure(info, AssertErrors.shouldHaveProperty(actual, key, value));
		}

		return myself;
	}

	/*
	 * (non-Javadoc)
	 * @see org.assertj.core.api.AbstractMapAssert#doesNotContainEntry(java.lang.Object, java.lang.Object)
	 */
	@Override
	public DocumentAssert doesNotContainEntry(String key, Object value) {

		Assert.hasText(key, "The key to look for must not be empty!");

		Lookup<?> lookup = lookup(key);

		if (lookup.isPathFound() && ObjectUtils.nullSafeEquals(value, lookup.getValue())) {
			throw Failures.instance().failure(info, AssertErrors.shouldNotHaveProperty(actual, key, value));
		}

		return myself;
	}

	/*
	 * (non-Javadoc)
	 * @see org.assertj.core.api.AbstractMapAssert#containsKey(java.lang.Object)
	 */
	@Override
	public DocumentAssert containsKey(String key) {
		return containsKeys(key);
	}

	/*
	 * (non-Javadoc)
	 * @see org.assertj.core.api.AbstractMapAssert#containsKeys(java.lang.Object[])
	 */
	@Override
	public final DocumentAssert containsKeys(String... keys) {

		Set<String> notFound = new LinkedHashSet<>();

		for (String key : keys) {

			if (!lookup(key).isPathFound()) {
				notFound.add(key);
			}
		}

		if (!notFound.isEmpty()) {
			throw Failures.instance().failure(info, shouldContainKeys(actual, notFound));
		}

		return myself;
	}

	/*
	 * (non-Javadoc)
	 * @see org.assertj.core.api.AbstractMapAssert#doesNotContainKey(java.lang.Object)
	 */
	@Override
	public DocumentAssert doesNotContainKey(String key) {
		return doesNotContainKeys(key);
	}

	/*
	 * (non-Javadoc)
	 * @see org.assertj.core.api.AbstractMapAssert#doesNotContainKeys(java.lang.Object[])
	 */
	@Override
	public final DocumentAssert doesNotContainKeys(String... keys) {

		Set<String> found = new LinkedHashSet<>();
		for (String key : keys) {

			if (lookup(key).isPathFound()) {
				found.add(key);
			}
		}
		if (!found.isEmpty()) {
			throw Failures.instance().failure(info, shouldNotContainKeys(actual, found));
		}

		return myself;
	}

	// override methods to annotate them with @SafeVarargs, we unfortunately can't do that in AbstractMapAssert as it is
	// used in soft assertions which need to be able to proxy method - @SafeVarargs requiring method to be final prevents
	// using proxies.

	/*
	 * (non-Javadoc)
	 * @see org.assertj.core.api.AbstractMapAssert#contains(java.util.Map.Entry[])
	 */
	@SafeVarargs
	@Override
	public final DocumentAssert contains(Map.Entry<? extends String, ? extends Object>... entries) {

		// if both actual and values are empty, then assertion passes.
		if (actual.isEmpty() && entries.length == 0) {
			return myself;
		}
		Set<Map.Entry<? extends String, ? extends Object>> notFound = new LinkedHashSet<>();
		for (Map.Entry<? extends String, ? extends Object> entry : entries) {
			if (!containsEntry(entry)) {
				notFound.add(entry);
			}
		}
		if (!notFound.isEmpty()) {
			throw Failures.instance().failure(info, shouldContain(actual, entries, notFound));
		}

		return myself;
	}

	/*
	 * (non-Javadoc)
	 * @see org.assertj.core.api.AbstractMapAssert#containsAnyOf(java.util.Map.Entry[])
	 */
	@SafeVarargs
	@Override
	public final DocumentAssert containsAnyOf(Map.Entry<? extends String, ? extends Object>... entries) {

		for (Map.Entry<? extends String, ? extends Object> entry : entries) {
			if (containsEntry(entry)) {
				return myself;
			}
		}

		throw Failures.instance().failure(info, ShouldContainAnyOf.shouldContainAnyOf(actual, entries));
	}

	/*
	 * (non-Javadoc)
	 * @see org.assertj.core.api.AbstractMapAssert#containsOnly(java.util.Map.Entry[])
	 */
	@SafeVarargs
	@Override
	public final DocumentAssert containsOnly(Map.Entry<? extends String, ? extends Object>... entries) {
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * @see org.assertj.core.api.AbstractMapAssert#doesNotContain(java.util.Map.Entry[])
	 */
	@SafeVarargs
	@Override
	public final DocumentAssert doesNotContain(Map.Entry<? extends String, ? extends Object>... entries) {

		Set<Map.Entry<? extends String, ? extends Object>> found = new LinkedHashSet<>();

		for (Map.Entry<? extends String, ? extends Object> entry : entries) {
			if (containsEntry(entry)) {
				found.add(entry);
			}
		}
		if (!found.isEmpty()) {
			throw Failures.instance().failure(info, shouldNotContain(actual, entries, found));
		}

		return myself;
	}

	/*
	 * (non-Javadoc)
	 * @see org.assertj.core.api.AbstractMapAssert#containsExactly(java.util.Map.Entry[])
	 */
	@SafeVarargs
	@Override
	public final DocumentAssert containsExactly(Map.Entry<? extends String, ? extends Object>... entries) {
		throw new UnsupportedOperationException();
	}

	private boolean containsEntry(Entry<? extends String, ?> entry) {

		Lookup<?> lookup = lookup(entry.getKey());

		return lookup.isPathFound() && ObjectUtils.nullSafeEquals(entry.getValue(), lookup.getValue());
	}

	private <T> Lookup<T> lookup(String path) {
		return lookup(actual, path);
	}

	@SuppressWarnings("unchecked")
	private static <T> Lookup<T> lookup(Bson source, String path) {

		String[] fragments = path.split("(?<!\\\\)\\.");

		if (fragments.length == 1) {

			Document document = (Document) source;
			String pathToUse = path.replace("\\.", ".");

			if (document.containsKey(pathToUse)) {
				return Lookup.found((T) document.get(pathToUse));
			}

			return Lookup.notFound();
		}

		Iterator<String> it = Arrays.asList(fragments).iterator();

		Object current = source;
		while (it.hasNext()) {

			String key = it.next().replace("\\.", ".");

			if (!(current instanceof Bson) && !key.startsWith("[")) {
				return Lookup.found(null);
			}

			if (key.startsWith("[")) {

				String indexNumber = key.substring(1, key.indexOf("]"));

				if (current instanceof List) {
					current = ((List) current).get(Integer.valueOf(indexNumber));
				}

				if (!it.hasNext()) {
					return Lookup.found((T) current);
				}
			} else {

				if (current instanceof Document) {

					Document document = (Document) current;

					if (!it.hasNext() && !document.containsKey(key)) {
						return Lookup.notFound();
					}

					current = document.get(key);
				}

				if (!it.hasNext()) {
					return Lookup.found((T) current);
				}
			}
		}

		return Lookup.notFound();
	}

	/*
	 * (non-Javadoc)
	 * @see org.assertj.core.api.AbstractMapAssert#hasEntrySatisfying(java.lang.Object, org.assertj.core.api.Condition)
	 */
	@Override
	public DocumentAssert hasEntrySatisfying(String key, Condition<? super Object> valueCondition) {

		Lookup<Object> value = lookup(key);

		if (!value.isPathFound() || !valueCondition.matches(value.getValue())) {
			throw Failures.instance().failure(info, elementsShouldBe(actual, value, valueCondition));
		}

		return myself;
	}

	/*
	 * (non-Javadoc)
	 * @see org.assertj.core.api.AbstractMapAssert#hasEntrySatisfying(java.lang.Object, java.util.function.Consumer)
	 */
	@Override
	public DocumentAssert hasEntrySatisfying(String key, Consumer<? super Object> valueRequirements) {

		containsKey(key);

		valueRequirements.accept(lookup(key).getValue());

		return myself;
	}

	@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
	@Getter
	static class Lookup<T> {

		private final T value;
		private final boolean pathFound;

		/**
		 * Factory method to construct a lookup with a hit.
		 *
		 * @param value the actual value.
		 * @return the lookup object.
		 */
		static <T> Lookup<T> found(T value) {
			return new Lookup<>(value, true);
		}

		/**
		 * Factory method to construct a lookup that yielded no match.
		 *
		 * @return the lookup object.
		 */
		static <T> Lookup<T> notFound() {
			return new Lookup<>(null, false);
		}
	}
}
