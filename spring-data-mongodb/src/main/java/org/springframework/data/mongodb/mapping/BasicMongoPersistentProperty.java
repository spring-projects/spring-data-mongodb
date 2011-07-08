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
package org.springframework.data.mongodb.mapping;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.math.BigInteger;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.types.ObjectId;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.model.AnnotationBasedPersistentProperty;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.util.StringUtils;

import com.mongodb.DBObject;

/**
 * Mongo specific {@link org.springframework.data.mapping.PersistentProperty} implementation.
 * 
 * @author Oliver Gierke
 */
public class BasicMongoPersistentProperty extends AnnotationBasedPersistentProperty<MongoPersistentProperty> implements
		MongoPersistentProperty {

	private static final Log LOG = LogFactory.getLog(BasicMongoPersistentProperty.class);

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

		org.springframework.data.mongodb.mapping.Field annotation = getField().getAnnotation(
				org.springframework.data.mongodb.mapping.Field.class);
		return annotation != null && StringUtils.hasText(annotation.value()) ? annotation.value() : field.getName();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.mapping.MongoPersistentProperty#getFieldOrder()
	 */
	public int getFieldOrder() {
		org.springframework.data.mongodb.mapping.Field annotation = getField().getAnnotation(
				org.springframework.data.mongodb.mapping.Field.class);
		return annotation != null ? annotation.order() : Integer.MAX_VALUE;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mapping.AbstractPersistentProperty#createAssociation()
	 */
	@Override
	protected Association<MongoPersistentProperty> createAssociation() {
		return new Association<MongoPersistentProperty>(this, null);
	}

	/* (non-Javadoc)
		 * @see org.springframework.data.mongodb.mapping.MongoPersistentProperty#isDbReference()
		 */
	public boolean isDbReference() {
		return getField().isAnnotationPresent(DBRef.class);
	}

	/* (non-Javadoc)
		 * @see org.springframework.data.mongodb.mapping.MongoPersistentProperty#getDBRef()
		 */
	public DBRef getDBRef() {
		return getField().getAnnotation(DBRef.class);
	}
}
