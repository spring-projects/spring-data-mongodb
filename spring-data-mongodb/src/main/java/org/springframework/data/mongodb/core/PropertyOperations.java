/*
 * Copyright 2018-2019 the original author or authors.
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

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

import org.bson.Document;
import org.springframework.data.mapping.SimplePropertyHandler;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.ProjectionInformation;
import org.springframework.util.ClassUtils;

/**
 * Common operations performed on properties of an entity like extracting fields information for projection creation.
 *
 * @author Christoph Strobl
 * @since 2.1
 */
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
class PropertyOperations {

	private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;

	/**
	 * For cases where {@code fields} is {@link Document#isEmpty() empty} include only fields that are required for
	 * creating the projection (target) type if the {@code targetType} is a {@literal DTO projection} or a
	 * {@literal closed interface projection}.
	 *
	 * @param projectionFactory must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @param domainType must not be {@literal null}.
	 * @param targetType must not be {@literal null}.
	 * @return {@link Document} with fields to be included.
	 */
	Document computeFieldsForProjection(ProjectionFactory projectionFactory, Document fields, Class<?> domainType,
			Class<?> targetType) {

		if (!fields.isEmpty() || ClassUtils.isAssignable(domainType, targetType)) {
			return fields;
		}

		Document projectedFields = new Document();

		if (targetType.isInterface()) {

			ProjectionInformation projectionInformation = projectionFactory.getProjectionInformation(targetType);

			if (projectionInformation.isClosed()) {
				projectionInformation.getInputProperties().forEach(it -> projectedFields.append(it.getName(), 1));
			}
		} else {
			mappingContext.getRequiredPersistentEntity(targetType).doWithProperties(
					(SimplePropertyHandler) persistentProperty -> projectedFields.append(persistentProperty.getName(), 1));
		}

		return projectedFields;
	}
}
