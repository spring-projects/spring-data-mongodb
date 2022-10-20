/*
 * Copyright 2022 the original author or authors.
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

import org.springframework.data.aot.TypeUtils;
import org.springframework.data.mongodb.core.mapping.MongoSimpleTypes;
import org.springframework.data.repository.util.ReactiveWrappers;
import org.springframework.data.repository.util.ReactiveWrappers.ReactiveLibrary;

/**
 * @author Christoph Strobl
 * @since 4.0
 */
public class MongoAotPredicates {

	public static final Predicate<Class<?>> IS_SIMPLE_TYPE = (type) ->  MongoSimpleTypes.HOLDER.isSimpleType(type) || TypeUtils.type(type).isPartOf("org.bson");
	public static final Predicate<ReactiveLibrary> IS_REACTIVE_LIBARARY_AVAILABLE = (lib) -> ReactiveWrappers.isAvailable(lib);

	public static boolean isReactorPresent() {
		return IS_REACTIVE_LIBARARY_AVAILABLE.test(ReactiveWrappers.ReactiveLibrary.PROJECT_REACTOR);
	}

}
