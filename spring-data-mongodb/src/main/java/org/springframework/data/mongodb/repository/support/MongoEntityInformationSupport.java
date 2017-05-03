/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.repository.support;

import org.springframework.data.domain.Persistable;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Support class responsible for creating {@link MongoEntityInformation} instances for a given
 * {@link MongoPersistentEntity}.
 *
 * @author Christoph Strobl
 * @since 1.10
 */
final class MongoEntityInformationSupport {

	private MongoEntityInformationSupport() {}

	/**
	 * Factory method for creating {@link MongoEntityInformation}.
	 *
	 * @param entity must not be {@literal null}.
	 * @param idType can be {@literal null}.
	 * @return never {@literal null}.
	 */
	@SuppressWarnings("unchecked")
	static <T, ID> MongoEntityInformation<T, ID> entityInformationFor(MongoPersistentEntity<?> entity, Class<?> idType) {

		Assert.notNull(entity, "Entity must not be null!");

		MappingMongoEntityInformation<T, ID> entityInformation = new MappingMongoEntityInformation<T, ID>(
				(MongoPersistentEntity<T>) entity, (Class<ID>) idType);

		return ClassUtils.isAssignable(Persistable.class, entity.getType())
				? new PersistableMongoEntityInformation<T, ID>(entityInformation) : entityInformation;
	}
}
