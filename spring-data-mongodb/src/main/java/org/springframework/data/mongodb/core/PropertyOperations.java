/*
 * Copyright 2018-2023 the original author or authors.
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
package org.springframework.data.mongodb.core;

import org.bson.Document;

import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.mapping.PersistentPropertyTranslator;
import org.springframework.data.projection.EntityProjection;
import org.springframework.data.util.Predicates;

/**
 * Common operations performed on properties of an entity like extracting fields information for projection creation.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.1
 */
class PropertyOperations {

	private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;

	PropertyOperations(MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext) {
		this.mappingContext = mappingContext;
	}

	/**
	 * For cases where {@code fields} is {@link Document#isEmpty() empty} include only fields that are required for
	 * creating the projection (target) type if the {@code EntityProjection} is a {@literal DTO projection} or a
	 * {@literal closed interface projection}.
	 *
	 * @param projection must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return {@link Document} with fields to be included.
	 */
	Document computeMappedFieldsForProjection(EntityProjection<?, ?> projection,
			Document fields) {

		if (!projection.isClosedProjection()) {
			return fields;
		}

		Document projectedFields = new Document();

		if (projection.getMappedType().getType().isInterface()) {
			projection.forEach(it -> {
				projectedFields.put(it.getPropertyPath().getSegment(), 1);
			});
		} else {

			// DTO projections use merged metadata between domain type and result type
			PersistentPropertyTranslator translator = PersistentPropertyTranslator.create(
					mappingContext.getRequiredPersistentEntity(projection.getDomainType()),
					Predicates.negate(MongoPersistentProperty::hasExplicitFieldName));

			MongoPersistentEntity<?> persistentEntity = mappingContext
					.getRequiredPersistentEntity(projection.getMappedType());
			for (MongoPersistentProperty property : persistentEntity) {
				projectedFields.put(translator.translate(property).getFieldName(), 1);
			}
		}

		return projectedFields;
	}
}
