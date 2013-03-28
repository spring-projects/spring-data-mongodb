/*
 * Copyright 2011-2013 the original author or authors.
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
package org.springframework.data.mongodb.core.mapping;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;

import org.springframework.data.mapping.model.SimpleTypeHolder;

/**
 * {@link MongoPersistentProperty} caching access to {@link #isIdProperty()} and {@link #getFieldName()}.
 * 
 * @author Oliver Gierke
 */
public class CachingMongoPersistentProperty extends BasicMongoPersistentProperty {

	private Boolean isIdProperty;
	private Boolean isAssociation;
	private String fieldName;

	/**
	 * Creates a new {@link CachingMongoPersistentProperty}.
	 * 
	 * @param field
	 * @param propertyDescriptor
	 * @param owner
	 * @param simpleTypeHolder
	 * @param fieldNamingStrategy
	 */
	public CachingMongoPersistentProperty(Field field, PropertyDescriptor propertyDescriptor,
			MongoPersistentEntity<?> owner, SimpleTypeHolder simpleTypeHolder, FieldNamingStrategy fieldNamingStrategy) {
		super(field, propertyDescriptor, owner, simpleTypeHolder, fieldNamingStrategy);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.BasicMongoPersistentProperty#isIdProperty()
	 */
	@Override
	public boolean isIdProperty() {

		if (this.isIdProperty == null) {
			this.isIdProperty = super.isIdProperty();
		}

		return this.isIdProperty;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.BasicMongoPersistentProperty#isAssociation()
	 */
	@Override
	public boolean isAssociation() {
		if (this.isAssociation == null) {
			this.isAssociation = super.isAssociation();
		}
		return this.isAssociation;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.BasicMongoPersistentProperty#getFieldName()
	 */
	@Override
	public String getFieldName() {

		if (this.fieldName == null) {
			this.fieldName = super.getFieldName();
		}

		return this.fieldName;
	}
}
