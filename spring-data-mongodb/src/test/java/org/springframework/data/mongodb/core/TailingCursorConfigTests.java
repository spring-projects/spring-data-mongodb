/*
 * Copyright 2010-2012 the original author or authors.
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
package org.springframework.data.mongodb.core;

import junit.framework.Assert;

import org.junit.Test;

/**
 * The test class for {@link TailingCursorConfig}
 *
 * @author Amol Nayak
 *
 * @since 1.1
 */
public class TailingCursorConfigTests {

	@Test
	public void withThousandMilliInterval() {
		TailingCursorConfig config = new TailingCursorConfig()
										.withRegenerationDelay(1000L);
		Assert.assertEquals(1000L, config.getCursorRegenerationInterval());
	}

	@Test
	public void withNegativeMilliInterval() {
		try {
			@SuppressWarnings("unused")
			TailingCursorConfig config = new TailingCursorConfig()
											.withRegenerationDelay(-1L);
		} catch (IllegalArgumentException e) {
			Assert.assertEquals("Provide a non negative value for cursor regeneration interval", e.getMessage());
		}
	}


	@Test
	public void withNonNullCollectionName() {
		TailingCursorConfig config = new TailingCursorConfig()
											.withCollectionName("CollectionName");
		Assert.assertEquals("CollectionName", config.getCollectionName());
	}


	@Test
	public void withNullCollection() {
		try {
			@SuppressWarnings("unused")
			TailingCursorConfig config = new TailingCursorConfig()
										.withCollectionName(null);
		} catch (IllegalArgumentException e) {
			Assert.assertEquals("Provide a non null non empty string for collection name", e.getMessage());
		}
	}

	@Test
	public void withNonNullIncreasingKey() {
		TailingCursorConfig config = new TailingCursorConfig()
			.withIncreasingKey("IncreasingKey");
		Assert.assertEquals("IncreasingKey", config.getIncreasingKey());
	}

	@Test
	public void withNullIncreasingKey() {
		try {
			@SuppressWarnings("unused")
			TailingCursorConfig config = new TailingCursorConfig()
				.withIncreasingKey(null);
		} catch (IllegalArgumentException e) {
			Assert.assertEquals("The increasing key should be a non null non empty string", e.getMessage());
		}
	}
}
