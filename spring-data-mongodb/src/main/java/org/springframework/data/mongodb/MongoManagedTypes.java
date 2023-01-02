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
package org.springframework.data.mongodb;

import java.util.Arrays;
import java.util.function.Consumer;

import org.springframework.data.domain.ManagedTypes;

/**
 * @author Christoph Strobl
 * @since 4.0
 */
public final class MongoManagedTypes implements ManagedTypes {

	private final ManagedTypes delegate;

	private MongoManagedTypes(ManagedTypes types) {
		this.delegate = types;
	}

	/**
	 * Wraps an existing {@link ManagedTypes} object with {@link MongoManagedTypes}.
	 *
	 * @param managedTypes
	 * @return
	 */
	public static MongoManagedTypes from(ManagedTypes managedTypes) {
		return new MongoManagedTypes(managedTypes);
	}

	/**
	 * Factory method used to construct {@link MongoManagedTypes} from the given array of {@link Class types}.
	 *
	 * @param types array of {@link Class types} used to initialize the {@link ManagedTypes}; must not be {@literal null}.
	 * @return new instance of {@link MongoManagedTypes} initialized from {@link Class types}.
	 */
	public static MongoManagedTypes from(Class<?>... types) {
		return fromIterable(Arrays.asList(types));
	}

	/**
	 * Factory method used to construct {@link MongoManagedTypes} from the given, required {@link Iterable} of
	 * {@link Class types}.
	 *
	 * @param types {@link Iterable} of {@link Class types} used to initialize the {@link ManagedTypes}; must not be
	 *          {@literal null}.
	 * @return new instance of {@link MongoManagedTypes} initialized the given, required {@link Iterable} of {@link Class
	 *         types}.
	 */
	public static MongoManagedTypes fromIterable(Iterable<? extends Class<?>> types) {
		return from(ManagedTypes.fromIterable(types));
	}

	/**
	 * Factory method to return an empty {@link MongoManagedTypes} object.
	 *
	 * @return an empty {@link MongoManagedTypes} object.
	 */
	public static MongoManagedTypes empty() {
		return from(ManagedTypes.empty());
	}

	@Override
	public void forEach(Consumer<Class<?>> action) {
		delegate.forEach(action);
	}
}
