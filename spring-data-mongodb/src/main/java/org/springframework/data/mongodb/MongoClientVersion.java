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
package org.springframework.data.mongodb;

import static org.springframework.util.ClassUtils.*;

/**
 * {@link MongoClientVersion} holds information about the used mongo-java client and is used to distinguish between
 * different versions.
 * 
 * @author Christoph Strobl
 * @since 1.7
 */
public class MongoClientVersion {

	private static final boolean IS_MONGO_30 = isPresent("com.mongodb.binding.SingleServerBinding",
			MongoClientVersion.class.getClassLoader());
	private static final boolean IS_ASYNC_CLIENT = isPresent("com.mongodb.async.client.MongoClient",
			MongoClientVersion.class.getClassLoader());

	/**
	 * @return true if mongo-java-driver version 3 or later is on classpath.
	 */
	public static boolean isMongo3Driver() {
		return IS_MONGO_30;
	}

	/**
	 * @return true if mongodb-driver-async is on classpath.
	 */
	public static boolean isAsyncClient() {
		return IS_ASYNC_CLIENT;
	}
}
