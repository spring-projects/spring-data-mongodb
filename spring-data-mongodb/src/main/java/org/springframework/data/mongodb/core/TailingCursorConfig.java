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

import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.mongodb.MongoException.CursorNotFound;

/**
 * The class that will be holding various configuration parameter values for the tailing cursor.
 *
 * @author Amol Nayak
 *
 * @since 1.1
 *
 */
public class TailingCursorConfig {

	private long cursorRegenerationInterval = 100L;	//defaults to 100 ms

	private String collectionName;

	private String increasingKey;

	/**
	 * The interval after which the cursor will be reopened on the collection if it reached
	 * the end on previous occasion or threw a {@link CursorNotFound} exception.
	 *
	 * @param cursorRegenerationInterval
	 * @return
	 */
	public TailingCursorConfig withRegenerationDelay(long cursorRegenerationInterval) {
		Assert.isTrue(cursorRegenerationInterval > 0,"Provide a non negative value for cursor regeneration interval");
		this.cursorRegenerationInterval = cursorRegenerationInterval;
		return this;
	}

	/**
	 * The name of the capped collection to be tailed.
	 *
	 * @param collectionName
	 * @return
	 */
	public TailingCursorConfig withCollectionName(String collectionName) {
		Assert.isTrue(StringUtils.hasText(collectionName), "Provide a non null non empty string for collection name");
		this.collectionName = collectionName;
		return this;
	}

	/**
	 * The name of the increasing key that would be used for comparison for tailing
	 * the collection.
	 *
	 * @return
	 */
	public TailingCursorConfig withIncreasingKey(String increasingKey) {
		Assert.isTrue(StringUtils.hasText(increasingKey),"The increasing key should be a non null non empty string");
		this.increasingKey = increasingKey;
		return this;
	}

	/**
	 * Gets the cursor regeneration interval.
	 *
	 * @return
	 */
	public long getCursorRegenerationInterval() {
		return cursorRegenerationInterval;
	}


	/**
	 * Gets the name of the collection that is to be tailed.
	 *
	 * @return
	 */
	public String getCollectionName() {
		return collectionName;
	}


	/**
	 * Gets the key which is in a naturally increasing order in the collection. This key will
	 * be used for comparison while tailing the collection.
	 *
	 * @return
	 */
	public String getIncreasingKey() {
		return increasingKey;
	}
}
