/*
 * Copyright 2015 the original author or authors.
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
package org.springframework.data.domain;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * @author Christoph Strobl
 * @param <T>
 */
public class Example<T> {

	private final T probe;

	private NullHandling nullHandling = NullHandling.IGNORE_NULL;
	private StringMatcher defaultStringMatcher = StringMatcher.DEFAULT;

	private boolean ignoreCase = false;

	private Map<String, PropertySpecifier> propertySpecifiers = new LinkedHashMap<String, PropertySpecifier>();

	public <S extends T> Example(S probe) {

		Assert.notNull(probe, "Probe must not be null!");
		this.probe = probe;
	}

	public T getProbe() {
		return probe;
	}

	public NullHandling getNullHandling() {
		return nullHandling;
	}

	public StringMatcher getDefaultStringMatcher() {
		return defaultStringMatcher;
	}

	public boolean isIngnoreCaseEnabled() {
		return this.ignoreCase;
	}

	public boolean hasPropertySpecifier(String path) {
		return propertySpecifiers.containsKey(path);
	}

	public PropertySpecifier getPropertySpecifier(String propertyPath) {
		return this.propertySpecifiers.get(propertyPath);
	}

	public boolean hasPropertySpecifiers() {
		return !this.propertySpecifiers.isEmpty();
	}

	@SuppressWarnings("unchecked")
	public Class<? extends T> getProbeType() {
		return (Class<? extends T>) ClassUtils.getUserClass(probe.getClass());
	}

	public static <S extends T, T> Example<T> exampleOf(S probe) {
		return new Example<T>(probe);
	}

	public static <S extends T, T> Example<T> exampleOf(S probe, String... ignoredProperties) {
		return new Builder<T>(probe).ignore(ignoredProperties).get();
	}

	public static <S extends T, T> Builder<S> newExampleOf(S probe) {
		return new Builder<S>(probe);
	}

	public static class Builder<T> {

		private Example<T> example;

		Builder(T probe) {
			example = new Example<T>(probe);
		}

		public Builder<T> with(NullHandling nullHandling) {
			return nullHandling(nullHandling);
		}

		public Builder<T> with(StringMatcher stringMatcher) {
			return stringMatcher(stringMatcher);
		}

		public Builder<T> with(PropertySpecifier specifier) {
			return specify(specifier);
		}

		public Builder<T> nullHandling(NullHandling nullHandling) {

			example.nullHandling = nullHandling == null ? NullHandling.IGNORE_NULL : nullHandling;
			return this;
		}

		public Builder<T> stringMatcher(StringMatcher stringMatcher) {

			example.defaultStringMatcher = stringMatcher == null ? StringMatcher.DEFAULT : stringMatcher;
			return this;
		}

		public Builder<T> stringMatcher(StringMatcher stringMatching, boolean ignoreCase) {

			example.defaultStringMatcher = stringMatching == null ? StringMatcher.DEFAULT : stringMatching;
			example.ignoreCase = ignoreCase;
			return this;
		}

		public Builder<T> ignoreCase() {
			example.ignoreCase = true;
			return this;
		}

		public Builder<T> specify(PropertySpecifier... specifiers) {

			for (PropertySpecifier specifier : specifiers) {
				example.propertySpecifiers.put(specifier.getPath(), specifier);
			}
			return this;
		}

		public Builder<T> ignore(String... ignoredProperties) {

			for (String ignoredProperty : ignoredProperties) {
				specify(PropertySpecifier.newPropertySpecifier(ignoredProperty)
						.valueTransformer(new ExcludingValueTransformer()).get());
			}
			return this;
		}

		public Example<T> get() {
			return this.example;
		}
	}

	/**
	 * Match modes indicates inclusion of complex objects.
	 * 
	 * @author Christoph Strobl
	 */
	public static enum NullHandling {
		/**
		 * Strict matching will use partially filled objects as reference.
		 */
		INCLUDE_NULL,
		/**
		 * Lenient matching will inspected nested objects and extract path if needed.
		 */
		IGNORE_NULL
	}

	/**
	 * Match modes indicates treatment of {@link String} values.
	 * 
	 * @author Christoph Strobl
	 */
	public static enum StringMatcher {

		/**
		 * Store specific default.
		 */
		DEFAULT,
		/**
		 * Matches the exact string
		 */
		EXACT,
		/**
		 * Matches string starting with pattern
		 */
		STARTING,
		/**
		 * Matches string ending with pattern
		 */
		ENDING,
		/**
		 * Matches string containing pattern
		 */
		CONTAINING,
		/**
		 * Treats strings as regular expression patterns
		 */
		REGEX
	}

	public static class ExcludingValueTransformer implements PropertyValueTransformer {

		@Override
		public Object tranform(Object source) {
			return null;
		}
	}

}
