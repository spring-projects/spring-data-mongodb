/*
 * Copyright 2015-2018 the original author or authors.
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
package org.springframework.data.mongodb.util;

import org.springframework.util.ClassUtils;

/**
 * {@link MongoClientVersion} holds information about the used mongo-java client and is used to distinguish between
 * different versions.
 *
 * @author Christoph Strobl
 * @since 1.7
 */
public class MongoClientVersion {

	private static final boolean SYNC_CLIENT_PRESENT = ClassUtils.isPresent("com.mongodb.MongoClient",
			MongoClientVersion.class.getClassLoader());

	private static final boolean ASYNC_CLIENT_PRESENT = ClassUtils.isPresent("com.mongodb.async.client.MongoClient",
			MongoClientVersion.class.getClassLoader());

	private static final boolean REACTIVE_CLIENT_PRESENT = ClassUtils
			.isPresent("com.mongodb.reactivestreams.client.MongoClient", MongoClientVersion.class.getClassLoader());

	/**
	 * @return {@literal true} if MongoDB Java driver version 3.0 or later is on classpath.
	 * @deprecated since 2.1, which requires MongoDB Java driver 3.8. Returns {@literal true} by default.
	 */
	@Deprecated
	public static boolean isMongo3Driver() {
		return true;
	}

	/**
	 * @return {@literal true} if MongoDB Java driver version 3.4 or later is on classpath.
	 * @since 1.10
	 * @deprecated since 2.1, which requires MongoDB Java driver 3.8. Returns {@literal true} by default.
	 */
	@Deprecated
	public static boolean isMongo34Driver() {
		return true;
	}

	/**
	 * @return {@literal true} if the async MongoDB Java driver is on classpath.
	 */
	public static boolean isAsyncClient() {
		return ASYNC_CLIENT_PRESENT;
	}

	/**
	 * @return {@literal true} if the sync MongoDB Java driver is on classpath.
	 * @since 2.1
	 */
	public static boolean isSyncClientPresent() {
		return SYNC_CLIENT_PRESENT;
	}

	/**
	 * @return {@literal true} if the reactive MongoDB Java driver is on classpath.
	 * @since 2.1
	 */
	public static boolean isReactiveClientPresent() {
		return REACTIVE_CLIENT_PRESENT;
	}
}
