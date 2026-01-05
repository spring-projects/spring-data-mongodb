/*
 * Copyright 2015-present the original author or authors.
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
package org.springframework.data.mongodb.util;

import org.jspecify.annotations.Nullable;
import org.springframework.data.util.Version;
import org.springframework.util.ClassUtils;

import com.mongodb.internal.build.MongoDriverVersion;

/**
 * {@link MongoClientVersion} holds information about the used mongo-java client and is used to distinguish between
 * different versions.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.7
 */
public class MongoClientVersion {

	private static final boolean SYNC_CLIENT_PRESENT = ClassUtils.isPresent("com.mongodb.MongoClient",
			MongoClientVersion.class.getClassLoader())
			|| ClassUtils.isPresent("com.mongodb.client.MongoClient", MongoClientVersion.class.getClassLoader());

	private static final boolean ASYNC_CLIENT_PRESENT = ClassUtils.isPresent("com.mongodb.async.client.MongoClient",
			MongoClientVersion.class.getClassLoader());

	private static final boolean REACTIVE_CLIENT_PRESENT = ClassUtils
			.isPresent("com.mongodb.reactivestreams.client.MongoClient", MongoClientVersion.class.getClassLoader());

	private static final boolean IS_VERSION_5_OR_NEWER;

	private static final Version CLIENT_VERSION;

	static {

		ClassLoader classLoader = MongoClientVersion.class.getClassLoader();
		Version version = getMongoDbDriverVersion(classLoader);

		CLIENT_VERSION = version;
		IS_VERSION_5_OR_NEWER = CLIENT_VERSION.isGreaterThanOrEqualTo(Version.parse("5.0"));
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

	/**
	 * @return {@literal true} if the MongoDB Java driver version is 5 or newer.
	 * @since 4.3
	 */
	public static boolean isVersion5orNewer() {
		return IS_VERSION_5_OR_NEWER;
	}

	private static Version getMongoDbDriverVersion(ClassLoader classLoader) {

		Version version = getVersionFromPackage(classLoader);
		return version == null ? guessDriverVersionFromClassPath(classLoader) : version;
	}

	private static @Nullable Version getVersionFromPackage(ClassLoader classLoader) {

		if (ClassUtils.isPresent("com.mongodb.internal.build.MongoDriverVersion", classLoader)) {
			try {
				return Version.parse(MongoDriverVersion.VERSION);
			} catch (IllegalArgumentException exception) {
				// well not much we can do, right?
			}
		}
		return null;
	}

	private static Version guessDriverVersionFromClassPath(ClassLoader classLoader) {

		if (ClassUtils.isPresent("com.mongodb.internal.connection.StreamFactoryFactory", classLoader)) {
			return Version.parse("5");
		}
		return Version.parse("4.11");
	}
}
