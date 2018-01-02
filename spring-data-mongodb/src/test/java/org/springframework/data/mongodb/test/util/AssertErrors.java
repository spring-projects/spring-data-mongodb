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

import org.assertj.core.error.BasicErrorMessageFactory;
import org.assertj.core.error.ErrorMessageFactory;
import org.assertj.core.internal.StandardComparisonStrategy;

/**
 * Utility class providing factory methods for {@link ErrorMessageFactory}.
 *
 * @author Mark Paluch
 */
class AssertErrors {

	/**
	 * Creates a new {@link ShouldHaveProperty}.
	 *
	 * @param actual the actual value in the failed assertion.
	 * @param key the key used in the failed assertion to compare the actual property key to.
	 * @param value the value used in the failed assertion to compare the actual property value to.
	 * @return the created {@link ErrorMessageFactory}.
	 */
	public static ErrorMessageFactory shouldHaveProperty(Object actual, String key, Object value) {
		return new ShouldHaveProperty(actual, key, value);
	}

	/**
	 * Creates a new {@link ShouldNotHaveProperty}.
	 *
	 * @param actual the actual value in the failed assertion.
	 * @param key the key used in the failed assertion to compare the actual property key to.
	 * @param value the value used in the failed assertion to compare the actual property value to.
	 * @return the created {@link ErrorMessageFactory}.
	 */
	public static ErrorMessageFactory shouldNotHaveProperty(Object actual, String key, Object value) {
		return new ShouldNotHaveProperty(actual, key, value);
	}

	private static class ShouldHaveProperty extends BasicErrorMessageFactory {

		private ShouldHaveProperty(Object actual, String key, Object value) {
			super("\n" + //
					"Expecting:\n" + //
					"  <%s>\n" + //
					"to have property with key:\n" + //
					"  <%s>\n" + //
					"and value:\n" + //
					"  <%s>\n" + //
					"%s", actual, key, value, StandardComparisonStrategy.instance());
		}
	}

	private static class ShouldNotHaveProperty extends BasicErrorMessageFactory {

		private ShouldNotHaveProperty(Object actual, String key, Object value) {
			super("\n" + //
					"Expecting:\n" + //
					"  <%s>\n" + //
					"not to have property with key:\n" + //
					"  <%s>\n" + //
					"and value:\n" + //
					"  <%s>\n" + //
					"but actually found such property %s", actual, key, value, StandardComparisonStrategy.instance());
		}
	}
}
