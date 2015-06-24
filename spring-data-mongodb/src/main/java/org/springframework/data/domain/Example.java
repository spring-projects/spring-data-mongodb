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

import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * @author Christoph Strobl
 * @param <T>
 */
public class Example<T> {

	private final T probe;
	private ObjectMatchMode objectMatchMode = ObjectMatchMode.LENIENT;
	private StringMatchMode stringMatchMode = StringMatchMode.DEFAULT;
	private boolean ignoreCaseEnabled = false;

	public <S extends T> Example(S probe) {

		Assert.notNull(probe, "Probe must not be null!");
		this.probe = probe;
	}

	public T getProbe() {
		return probe;
	}

	public ObjectMatchMode getObjectMatchMode() {
		return objectMatchMode;
	}

	public StringMatchMode getStringMatchMode() {
		return stringMatchMode;
	}

	public boolean isIngnoreCaseEnabled() {
		return this.ignoreCaseEnabled;
	}

	@SuppressWarnings("unchecked")
	public Class<? extends T> getProbeType() {
		return (Class<? extends T>) ClassUtils.getUserClass(probe.getClass());
	}

	public static <S extends T, T> Example<T> example(S probe) {
		return new Example<T>(probe);
	}

	public static class ExampleBuilder<T> {

		private Example<T> example;

		public ExampleBuilder(T probe) {
			example = new Example<T>(probe);
		}

		public ExampleBuilder<T> objectMatchMode(ObjectMatchMode matchMode) {

			example.objectMatchMode = matchMode == null ? ObjectMatchMode.LENIENT : matchMode;
			return this;
		}

		public ExampleBuilder<T> stringMatchMode(StringMatchMode matchMode) {

			example.stringMatchMode = matchMode == null ? StringMatchMode.DEFAULT : matchMode;
			return this;
		}

		public ExampleBuilder<T> stringMatchMode(StringMatchMode matchMode, boolean ignoreCase) {

			example.stringMatchMode = matchMode == null ? StringMatchMode.DEFAULT : matchMode;
			example.ignoreCaseEnabled = ignoreCase;
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
	public static enum ObjectMatchMode {
		/**
		 * Strict matching will use partially filled objects as reference.
		 */
		STRICT,
		/**
		 * Lenient matching will inspected nested objects and extract path if needed.
		 */
		LENIENT
	}

	/**
	 * Match modes indicates treatment of {@link String} values.
	 * 
	 * @author Christoph Strobl
	 */
	public static enum StringMatchMode {

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

	// TODO: add default null handling
	// TODO: add default String handling
	// TODO: add per field null handling
	// TODO: add per field String handling

}
