/*
 * Copyright 2022-2023 the original author or authors.
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
package org.springframework.data.mongodb.aot;

import java.util.function.Predicate;

import org.springframework.data.mongodb.core.mapping.MongoSimpleTypes;
import org.springframework.data.util.ReactiveWrappers;
import org.springframework.data.util.ReactiveWrappers.ReactiveLibrary;
import org.springframework.data.util.TypeUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.ClassUtils;

/**
 * @author Christoph Strobl
 * @since 4.0
 */
public class MongoAotPredicates {

	public static final Predicate<Class<?>> IS_SIMPLE_TYPE = (type) ->  MongoSimpleTypes.HOLDER.isSimpleType(type) || TypeUtils.type(type).isPartOf("org.bson");
	public static final Predicate<ReactiveLibrary> IS_REACTIVE_LIBARARY_AVAILABLE = ReactiveWrappers::isAvailable;
	public static final Predicate<ClassLoader> IS_SYNC_CLIENT_PRESENT = (classLoader) -> ClassUtils.isPresent("com.mongodb.client.MongoClient", classLoader);

	public static boolean isReactorPresent() {
		return IS_REACTIVE_LIBARARY_AVAILABLE.test(ReactiveWrappers.ReactiveLibrary.PROJECT_REACTOR);
	}

	public static boolean isSyncClientPresent(@Nullable ClassLoader classLoader) {
		return IS_SYNC_CLIENT_PRESENT.test(classLoader);
	}

}
