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

package org.springframework.data.document.mongodb.mapping;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

import org.bson.types.CodeWScope;
import org.bson.types.ObjectId;
import org.springframework.data.mapping.AbstractMappingContext;
import org.springframework.data.mapping.SimpleTypeHolder;
import org.springframework.data.util.TypeInformation;

/**
 * @author Jon Brisbin <jbrisbin@vmware.com>
 * @author Oliver Gierke ogierke@vmware.com
 */
public class MongoMappingContext extends AbstractMappingContext<BasicMongoPersistentEntity<?>, MongoPersistentProperty> {

	private static final Set<Class<?>> MONGO_SIMPLE_TYPES = new HashSet<Class<?>>();

	static {
		MONGO_SIMPLE_TYPES.add(com.mongodb.DBRef.class);
		MONGO_SIMPLE_TYPES.add(ObjectId.class);
		MONGO_SIMPLE_TYPES.add(CodeWScope.class);
		MONGO_SIMPLE_TYPES.add(Character.class);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.AbstractMappingContext#setSimpleTypeHolder(org.springframework.data.mapping.SimpleTypeHolder)
	 */
	@Override
	public void setSimpleTypeHolder(SimpleTypeHolder simpleTypes) {
		super.setSimpleTypeHolder(new SimpleTypeHolder(MONGO_SIMPLE_TYPES, simpleTypes));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.AbstractMappingContext#createPersistentProperty(java.lang.reflect.Field, java.beans.PropertyDescriptor, org.springframework.data.mapping.MutablePersistentEntity, org.springframework.data.mapping.SimpleTypeHolder)
	 */
	@Override
	public MongoPersistentProperty createPersistentProperty(Field field, PropertyDescriptor descriptor,
			BasicMongoPersistentEntity<?> owner, SimpleTypeHolder simpleTypeHolder) {
		return new BasicMongoPersistentProperty(field, descriptor, owner, simpleTypeHolder);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mapping.BasicMappingContext#createPersistentEntity(org.springframework.data.util.TypeInformation, org.springframework.data.mapping.model.MappingContext)
	 */
	@Override
	protected <T> BasicMongoPersistentEntity<T> createPersistentEntity(TypeInformation<T> typeInformation) {
		return new BasicMongoPersistentEntity<T>(typeInformation);
	}
}
