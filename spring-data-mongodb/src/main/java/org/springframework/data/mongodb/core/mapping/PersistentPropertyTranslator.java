/*
 * Copyright 2021-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.mapping;

import java.util.function.Predicate;

import org.springframework.data.util.Predicates;
import org.springframework.lang.Nullable;

/**
 * Utility to translate a {@link MongoPersistentProperty} into a corresponding property from a different
 * {@link MongoPersistentEntity} by looking it up by name.
 * <p>
 * Mainly used within the framework.
 *
 * @author Mark Paluch
 * @since 3.4
 */
public class PersistentPropertyTranslator {

	/**
	 * Translate a {@link MongoPersistentProperty} into a corresponding property from a different
	 * {@link MongoPersistentEntity}.
	 *
	 * @param property must not be {@literal null}.
	 * @return the translated property. Can be the original {@code property}.
	 */
	public MongoPersistentProperty translate(MongoPersistentProperty property) {
		return property;
	}

	/**
	 * Create a new {@link PersistentPropertyTranslator}.
	 *
	 * @param targetEntity must not be {@literal null}.
	 * @return the property translator to use.
	 */
	public static PersistentPropertyTranslator create(@Nullable MongoPersistentEntity<?> targetEntity) {
		return create(targetEntity, Predicates.isTrue());
	}

	/**
	 * Create a new {@link PersistentPropertyTranslator} accepting a {@link Predicate filter predicate} whether the
	 * translation should happen at all.
	 *
	 * @param targetEntity must not be {@literal null}.
	 * @param translationFilter must not be {@literal null}.
	 * @return the property translator to use.
	 */
	public static PersistentPropertyTranslator create(@Nullable MongoPersistentEntity<?> targetEntity,
			Predicate<MongoPersistentProperty> translationFilter) {
		return targetEntity != null ? new EntityPropertyTranslator(targetEntity, translationFilter)
				: new PersistentPropertyTranslator();
	}

	private static class EntityPropertyTranslator extends PersistentPropertyTranslator {

		private final MongoPersistentEntity<?> targetEntity;
		private final Predicate<MongoPersistentProperty> translationFilter;

		EntityPropertyTranslator(MongoPersistentEntity<?> targetEntity,
				Predicate<MongoPersistentProperty> translationFilter) {
			this.targetEntity = targetEntity;
			this.translationFilter = translationFilter;
		}

		@Override
		public MongoPersistentProperty translate(MongoPersistentProperty property) {

			if (!translationFilter.test(property)) {
				return property;
			}

			MongoPersistentProperty targetProperty = targetEntity.getPersistentProperty(property.getName());
			return targetProperty != null ? targetProperty : property;
		}
	}

}
