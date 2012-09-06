/*
 * Copyright 2011-2012 the original author or authors.
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
import java.math.BigInteger;
import java.util.HashSet;
import java.util.Set;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.model.AnnotationBasedPersistentProperty;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.util.StringUtils;

import com.mongodb.DBObject;

/**
 * MongoDB specific {@link org.springframework.data.mapping.MongoPersistentProperty} implementation.
 * 
 * @author Oliver Gierke
 * @author Patryk Wasik
 */
public class BasicMongoPersistentProperty extends AnnotationBasedPersistentProperty<MongoPersistentProperty> implements
		MongoPersistentProperty {

	private static final Logger LOG = LoggerFactory.getLogger(BasicMongoPersistentProperty.class);

	private static final String ID_FIELD_NAME = "_id";
	private static final Set<Class<?>> SUPPORTED_ID_TYPES = new HashSet<Class<?>>();
	private static final Set<String> SUPPORTED_ID_PROPERTY_NAMES = new HashSet<String>();

	static {
		SUPPORTED_ID_TYPES.add(ObjectId.class);
		SUPPORTED_ID_TYPES.add(String.class);
		SUPPORTED_ID_TYPES.add(BigInteger.class);

		SUPPORTED_ID_PROPERTY_NAMES.add("id");
		SUPPORTED_ID_PROPERTY_NAMES.add("_id");
	}

	/**
	 * Creates a new {@link BasicMongoPersistentProperty}.
	 * 
	 * @param field
	 * @param propertyDescriptor
	 * @param owner
	 * @param simpleTypeHolder
	 */
	public BasicMongoPersistentProperty(Field field, PropertyDescriptor propertyDescriptor,
			MongoPersistentEntity<?> owner, SimpleTypeHolder simpleTypeHolder) {
		super(field, propertyDescriptor, owner, simpleTypeHolder);

		if (isIdProperty() && getFieldName() != ID_FIELD_NAME) {
			LOG.warn("Customizing field name for id property not allowed! Custom name will not be considered!");
		}
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mapping.FooBasicPersistentProperty#isAssociation()
	 */
	@Override
	public boolean isAssociation() {
		return field.isAnnotationPresent(DBRef.class) || super.isAssociation();
	}

	/**
	 * Also considers fields as id that are of supported id type and name.
	 * 
	 * @see #SUPPORTED_ID_PROPERTY_NAMES
	 * @see #SUPPORTED_ID_TYPES
	 */
	@Override
	public boolean isIdProperty() {

		if (super.isIdProperty()) {
			return true;
		}

		// We need to support a wider range of ID types than just the ones that can be converted to an ObjectId
		return SUPPORTED_ID_PROPERTY_NAMES.contains(field.getName());
	}

	/**
	 * Returns the key to be used to store the value of the property inside a Mongo {@link DBObject}.
	 * 
	 * @return
	 */
	public String getFieldName() {

		if (isIdProperty()) {
			return ID_FIELD_NAME;
		}

		org.springframework.data.mongodb.core.mapping.Field annotation = getField().getAnnotation(
				org.springframework.data.mongodb.core.mapping.Field.class);
		return annotation != null && StringUtils.hasText(annotation.value()) ? annotation.value() : field.getName();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.MongoPersistentProperty#getFieldOrder()
	 */
	public int getFieldOrder() {
		org.springframework.data.mongodb.core.mapping.Field annotation = getField().getAnnotation(
				org.springframework.data.mongodb.core.mapping.Field.class);
		return annotation != null ? annotation.order() : Integer.MAX_VALUE;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.model.AbstractPersistentProperty#createAssociation()
	 */
	@Override
	protected Association<MongoPersistentProperty> createAssociation() {
		return new Association<MongoPersistentProperty>(this, null);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.MongoPersistentProperty#isDbReference()
	 */
	public boolean isDbReference() {
		return getField().isAnnotationPresent(DBRef.class);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.MongoPersistentProperty#getDBRef()
	 */
	public DBRef getDBRef() {
		return getField().getAnnotation(DBRef.class);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.MongoPersistentProperty#isVersionProperty()
	 */
	public boolean isVersionProperty() {
		return getField().isAnnotationPresent(Version.class);
	}
}
