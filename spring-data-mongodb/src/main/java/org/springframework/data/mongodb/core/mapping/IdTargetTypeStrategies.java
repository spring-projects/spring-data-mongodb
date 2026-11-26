/*
 * Copyright 2026-present the original author or authors.
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

import org.bson.Document;
import org.bson.types.ObjectId;
import org.jspecify.annotations.Nullable;

/**
 * Built-in {@link IdTargetTypeStrategy strategies}. Implemented as named classes, not lambdas, for readable class
 * names.
 *
 * @author Christoph Strobl
 * @since 5.3
 * @see IdTargetTypeStrategy#OBJECT_ID
 * @see IdTargetTypeStrategy#IMPLICIT
 */
class IdTargetTypeStrategies {

	static final IdTargetTypeStrategy OBJECT_ID = new ObjectIdStrategy();
	static final IdTargetTypeStrategy IMPLICIT = new ImplicitStrategy();

	private IdTargetTypeStrategies() {}

	static final class ObjectIdStrategy implements IdTargetTypeStrategy {

		@Override
		public Class<?> getIdTargetType(@Nullable MongoPersistentProperty property) {
			return ObjectId.class;
		}
	}

	static final class ImplicitStrategy implements IdTargetTypeStrategy {

		@Override
		public Class<?> getIdTargetType(@Nullable MongoPersistentProperty property) {

			if (property == null) {
				return FieldType.IMPLICIT.getJavaClass();
			}

			return property.isEntity() ? Document.class : property.getType();
		}
	}
}
