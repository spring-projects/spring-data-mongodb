/*
 * Copyright 2012-2020 the original author or authors.
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
package org.springframework.data.mapping.model;

import java.util.Collections;
import java.util.Map;

import org.springframework.data.mapping.PersistentEntity;
import org.springframework.util.Assert;

/**
 * Simple value object allowing access to {@link EntityInstantiator} instances for a given type falling back to a
 * default one.
 *
 * @author Oliver Drotbohm
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.3
 */
public class EntityInstantiators {

	private final EntityInstantiator fallback;
	private final Map<Class<?>, EntityInstantiator> customInstantiators;

	/**
	 * Creates a new {@link EntityInstantiators} using the default fallback instantiator and no custom ones.
	 */
	public EntityInstantiators() {
		this(Collections.emptyMap());
	}

	/**
	 * Creates a new {@link EntityInstantiators} using the given {@link EntityInstantiator} as fallback.
	 *
	 * @param fallback must not be {@literal null}.
	 */
	public EntityInstantiators(EntityInstantiator fallback) {
		this(fallback, Collections.emptyMap());
	}

	/**
	 * Creates a new {@link EntityInstantiators} using the default fallback instantiator and the given custom ones.
	 *
	 * @param customInstantiators must not be {@literal null}.
	 */
	public EntityInstantiators(Map<Class<?>, EntityInstantiator> customInstantiators) {
		this(new KotlinClassGeneratingEntityInstantiator(), customInstantiators);
	}

	/**
	 * Creates a new {@link EntityInstantiator} using the given fallback {@link EntityInstantiator} and the given custom
	 * ones.
	 *
	 * @param defaultInstantiator must not be {@literal null}.
	 * @param customInstantiators must not be {@literal null}.
	 */
	public EntityInstantiators(EntityInstantiator defaultInstantiator,
			Map<Class<?>, EntityInstantiator> customInstantiators) {

		Assert.notNull(defaultInstantiator, "DefaultInstantiator must not be null!");
		Assert.notNull(customInstantiators, "CustomInstantiators must not be null!");

		this.fallback = defaultInstantiator;
		this.customInstantiators = customInstantiators;
	}

	/**
	 * Returns the {@link EntityInstantiator} to be used to create the given {@link PersistentEntity}.
	 *
	 * @param entity must not be {@literal null}.
	 * @return will never be {@literal null}.
	 */
	public EntityInstantiator getInstantiatorFor(PersistentEntity<?, ?> entity) {

		Assert.notNull(entity, "Entity must not be null!");
		Class<?> type = entity.getType();

		if (!customInstantiators.containsKey(type)) {

			if (entity.getTypeInformation() instanceof EntiyInstantiatorAware) {
				return ((EntiyInstantiatorAware) entity.getTypeInformation()).getEntiyInstantiatorOrDefault(fallback);
			}
			return fallback;
		}

		EntityInstantiator instantiator = customInstantiators.get(entity.getType());
		return instantiator == null ? fallback : instantiator;
	}
}
