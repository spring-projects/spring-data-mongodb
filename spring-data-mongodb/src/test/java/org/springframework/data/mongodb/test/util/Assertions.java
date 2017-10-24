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

import org.bson.Document;

/**
 * The entry point for all MongoDB assertions. This class extends {@link org.assertj.core.api.Assertions} for
 * convenience to statically import a single class.
 *
 * @author Mark Paluch
 */
public abstract class Assertions extends org.assertj.core.api.Assertions {

	private Assertions() {
		// no instances allowed.
	}

	/**
	 * Create assertion for {@link Document}.
	 *
	 * @param actual the actual value.
	 * @return the created assertion object.
	 */
	public static DocumentAssert assertThat(Document document) {
		return new DocumentAssert(document);
	}
}
