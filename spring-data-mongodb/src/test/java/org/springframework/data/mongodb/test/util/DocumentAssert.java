/*
 * Copyright 2017-2021 the original author or authors.
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

	@Override
	public DocumentAssert containsEntry(String key, Object value) {

		Assert.hasText(key, "The key to look for must not be empty!");

		Lookup<?> lookup = lookup(key);

		if (!lookup.isPathFound() || !ObjectUtils.nullSafeEquals(value, lookup.getValue())) {
			throw Failures.instance().failure(info, AssertErrors.shouldHaveProperty(actual, key, value));
		}

		return myself;
	}

	/**
	 * Verifies that the actual value is equal to the given one by accepting the expected {@link Document} in its
	 * JSON/BSON representation.
	 * <p>
	 * Example:
	 *
	 * <pre>
	 * <code class='java'> // assertions will pass
	 * assertThat(Document.parse(&quot;{foo: 1}&quot;).isEqualTo(&quot;{foo: 1}&quot;);
	 * </pre>
	 *
	 * @param expectedBson the given value to compare the actual value to in BSON/JSON format.
	 * @return {@code this} assertion object.
	 * @throws AssertionError if the actual value is not equal to the given one.
	 * @see Document#parse(String)
	 */
	public DocumentAssert isEqualTo(String expectedBson) {

		isEqualTo(Document.parse(expectedBson));
		return myself;
	}

	@Override
	public DocumentAssert doesNotContainEntry(String key, Object value) {

		Assert.hasText(key, "The key to look for must not be empty!");

		Lookup<?> lookup = lookup(key);

		if (lookup.isPathFound() && ObjectUtils.nullSafeEquals(value, lookup.getValue())) {
			throw Failures.instance().failure(info, AssertErrors.shouldNotHaveProperty(actual, key, value));
		}

		return myself;
	}

	@Override
	public DocumentAssert containsKey(String key) {
		return containsKeys(key);
	}

	@Override
	protected DocumentAssert containsKeysForProxy(String[] keys) {

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

	@Override
	public DocumentAssert doesNotContainKey(String key) {
		return doesNotContainKeys(key);
	}

	@Override
	protected DocumentAssert doesNotContainKeysForProxy(String[] keys) {

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

	@Override
	protected DocumentAssert containsForProxy(Entry<? extends String, ?>[] entries) {

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

	@Override
	protected DocumentAssert containsAnyOfForProxy(Entry<? extends String, ?>[] entries) {
		for (Map.Entry<? extends String, ? extends Object> entry : entries) {
			if (containsEntry(entry)) {
				return myself;
			}
		}

		throw Failures.instance().failure(info, ShouldContainAnyOf.shouldContainAnyOf(actual, entries));
	}

	@Override
	protected DocumentAssert containsOnlyForProxy(Entry<? extends String, ?>[] entries) {
		throw new UnsupportedOperationException();
	}

	@Override
	protected DocumentAssert doesNotContainForProxy(Entry<? extends String, ?>[] entries) {
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

	@Override
	protected DocumentAssert containsExactlyForProxy(Entry<? extends String, ?>[] entries) {
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

		Document lookupDocument = (Document) source;
		String pathToUse = path.replace("\\.", ".");

		if (lookupDocument.containsKey(pathToUse)) {
			return Lookup.found((T) lookupDocument.get(pathToUse));
		}

		String[] fragments = path.split("(?<!\\\\)\\.");
		Iterator<String> it = Arrays.asList(fragments).iterator();

		Object current = source;
		while (it.hasNext()) {

			String key = it.next().replace("\\.", ".");

			if ((!(current instanceof Bson) && !(current instanceof Map)) && !key.startsWith("[")) {
				return Lookup.notFound();
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

				else if (current instanceof Map) {

					Map document = (Map) current;

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

	@Override
	public DocumentAssert hasEntrySatisfying(String key, Condition<? super Object> valueCondition) {

		Lookup<Object> value = lookup(key);

		if (!value.isPathFound() || !valueCondition.matches(value.getValue())) {
			throw Failures.instance().failure(info, elementsShouldBe(actual, value, valueCondition));
		}

		return myself;
	}

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
