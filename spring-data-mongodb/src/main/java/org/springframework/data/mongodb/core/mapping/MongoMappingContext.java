/*
 * Copyright (c) 2011 by the original author(s).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.mongodb.core.mapping;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.data.mapping.context.AbstractMappingContext;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.util.TypeInformation;

/**
 * @author Jon Brisbin <jbrisbin@vmware.com>
 * @author Oliver Gierke ogierke@vmware.com
 */
public class MongoMappingContext extends AbstractMappingContext<BasicMongoPersistentEntity<?>, MongoPersistentProperty>
		implements ApplicationContextAware {

	private ApplicationContext context;

	/**
	 * Creates a new {@link MongoMappingContext}.
	 */
	public MongoMappingContext() {
		setSimpleTypeHolder(MongoSimpleTypes.HOLDER);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.AbstractMappingContext#createPersistentProperty(java.lang.reflect.Field, java.beans.PropertyDescriptor, org.springframework.data.mapping.MutablePersistentEntity, org.springframework.data.mapping.SimpleTypeHolder)
	 */
	@Override
	public MongoPersistentProperty createPersistentProperty(Field field, PropertyDescriptor descriptor,
			BasicMongoPersistentEntity<?> owner, SimpleTypeHolder simpleTypeHolder) {
		return new CachingMongoPersistentProperty(field, descriptor, owner, simpleTypeHolder);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.BasicMappingContext#createPersistentEntity(org.springframework.data.util.TypeInformation, org.springframework.data.mapping.model.MappingContext)
	 */
	@Override
	protected <T> BasicMongoPersistentEntity<T> createPersistentEntity(TypeInformation<T> typeInformation) {

		BasicMongoPersistentEntity<T> entity = new BasicMongoPersistentEntity<T>(typeInformation);

		if (context != null) {
			entity.setApplicationContext(context);
		}

		return entity;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.context.ApplicationContextAware#setApplicationContext(org.springframework.context.ApplicationContext)
	 */
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.context = applicationContext;
	}
}
