/*
 * Copyright 2017 the original author or authors.
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

import lombok.RequiredArgsConstructor;

import org.assertj.core.api.Condition;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;

/**
 * All the things that cannot be done out of the box with {@link org.assertj.core.api.Assertions AssertJ}.
 *
 * @author Christoph Strobl
 */
public class Conditions {

	/**
	 * Get a {@link Condition} capable of asserting against a given {@link Matcher hamcest Matcher}.
	 *
	 * @param matcher must not be {@literal null}.
	 * @param <T>
	 * @return
	 */
	public static <T> Condition<T> matchedBy(Matcher<? super T> matcher) {
		return new HamcrestCondition(matcher);
	}

	@RequiredArgsConstructor
	static class HamcrestCondition<T> extends Condition<T> {

		final Matcher<? super T> matcher;

		public boolean matches(T value) {

			if (matcher.matches(value)) {
				return true;
			}

			setErrorMessage(value);
			return false;
		}

		private void setErrorMessage(T value) {

			StringDescription sd = new StringDescription();
			matcher.describeTo(sd);
			as(sd.toString(), value);
		}
	}
}
