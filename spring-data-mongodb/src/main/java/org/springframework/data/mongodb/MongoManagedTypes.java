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
package org.springframework.data.mongodb;

import java.util.function.Consumer;

import org.springframework.data.domain.ManagedTypes;

/**
 * @author Christoph Strobl
 * @since 4.0
 */
public final class MongoManagedTypes implements ManagedTypes {

	private final ManagedTypes delegate;

	public MongoManagedTypes(ManagedTypes types) {
		this.delegate = types;
	}

	public static MongoManagedTypes from(ManagedTypes managedTypes) {
		return new MongoManagedTypes(managedTypes);
	}

	public static MongoManagedTypes of(Iterable<? extends Class<?>> types) {
		return from(ManagedTypes.fromIterable(types));
	}

	public static MongoManagedTypes none() {
		return from(ManagedTypes.empty());
	}

	@Override
	public void forEach(Consumer<Class<?>> action) {
		delegate.forEach(action);
	}
}
