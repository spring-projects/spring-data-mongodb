/*
 * Copyright 2011 the original author or authors.
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

import java.io.Serializable;

import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.repository.query.EntityInformationCreator;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.util.Assert;

/**
 * Simple {@link EntityInformationCreator} to to create {@link MongoEntityInformation} instances based on a
 * {@link MappingContext}.
 * 
 * @author Oliver Gierke
 */
public class DefaultEntityInformationCreator implements EntityInformationCreator {

	private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;

	public DefaultEntityInformationCreator(
			MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext) {
		Assert.notNull(mappingContext);
		this.mappingContext = mappingContext;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.support.EntityInformationCreator#getEntityInformation(java.lang.Class)
	 */
	public <T, ID extends Serializable> MongoEntityInformation<T, ID> getEntityInformation(Class<T> domainClass) {
		return getEntityInformation(domainClass, null);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.support.EntityInformationCreator#getEntityInformation(java.lang.Class, java.lang.Class)
	 */
	@SuppressWarnings("unchecked")
	public <T, ID extends Serializable> MongoEntityInformation<T, ID> getEntityInformation(Class<T> domainClass,
			Class<?> collectionClass) {

		MongoPersistentEntity<T> persistentEntity = (MongoPersistentEntity<T>) mappingContext
				.getPersistentEntity(domainClass);
		String customCollectionName = collectionClass == null ? null : mappingContext.getPersistentEntity(collectionClass)
				.getCollection();

		return new MappingMongoEntityInformation<T, ID>(persistentEntity, customCollectionName);
	}
}