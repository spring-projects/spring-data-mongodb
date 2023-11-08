/*
 * Copyright 2018-2023 the original author or authors.
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

import org.springframework.data.domain.Persistable;

/**
 * @author Christoph Strobl
 * @currentRead Shadow's Edge - Brent Weeks
 */
public class AfterTransactionAssertion<T extends Persistable> {

	private final T persistable;
	private boolean expectToBePresent;

	public AfterTransactionAssertion(T persistable) {
		this.persistable = persistable;
	}

	public void isPresent() {
		expectToBePresent = true;
	}

	public void isNotPresent() {
		expectToBePresent = false;
	}

	public Object getId() {
		return persistable.getId();
	}

	public boolean shouldBePresent() {
		return expectToBePresent;
	}

	public T getPersistable() {
		return this.persistable;
	}

	public boolean isExpectToBePresent() {
		return this.expectToBePresent;
	}

	public void setExpectToBePresent(boolean expectToBePresent) {
		this.expectToBePresent = expectToBePresent;
	}
}
