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

import org.springframework.data.domain.Example.StringMatcher;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 */
public class PropertySpecifier {

	private PropertyValueTransformer valueTransformer;
	private StringMatcher stringMatcher = StringMatcher.DEFAULT;
	private boolean ignoreCase = false;
	private final String path;

	PropertySpecifier(String path) {

		Assert.hasText(path, "Path must not be null/empty!");
		this.path = path;
	}

	public StringMatcher getStringMatcher() {
		return this.stringMatcher == null ? StringMatcher.DEFAULT : stringMatcher;
	}

	public boolean isIgnoreCaseEnabled() {
		return ignoreCase;
	}

	public PropertyValueTransformer getPropertyValueTransformer() {
		return valueTransformer == null ? NoOpPropertyValueTransformer.INSTANCE : valueTransformer;
	}

	public String getPath() {
		return path;
	}

	public Object transformValue(Object source) {
		return getPropertyValueTransformer().tranform(source);
	}

	public static PropertySpecifier ignoreCase(String propertyPath) {
		return new Builder(propertyPath).ignoreCase().get();
	}

	public static Builder newPropertySpecifier(String propertyPath) {
		return new Builder(propertyPath);
	}

	public static class Builder {

		private PropertySpecifier specifier;

		Builder(String path) {
			specifier = new PropertySpecifier(path);
		}

		public Builder with(StringMatcher stringMatcher) {
			return stringMatcher(stringMatcher);
		}

		public Builder with(PropertyValueTransformer valueTransformer) {
			return valueTransformer(valueTransformer);
		}

		public Builder stringMatcher(StringMatcher stringMatcher) {
			specifier.stringMatcher = stringMatcher;
			return this;
		}

		public Builder ignoreCase() {
			specifier.ignoreCase = true;
			return this;
		}

		public Builder valueTransformer(PropertyValueTransformer valueTransformer) {
			specifier.valueTransformer = valueTransformer;
			return this;
		}

		public PropertySpecifier get() {
			return this.specifier;
		}
	}

	static enum NoOpPropertyValueTransformer implements PropertyValueTransformer {

		INSTANCE;

		@Override
		public Object tranform(Object source) {
			return source;
		}

	}
}
