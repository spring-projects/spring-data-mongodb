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
package org.springframework.data.document.mongodb.convert;

import static org.springframework.beans.PropertyAccessorFactory.forBeanPropertyAccess;
import static org.springframework.beans.PropertyAccessorFactory.forDirectFieldAccess;

import org.springframework.beans.BeanWrapper;
import org.springframework.beans.ConfigurablePropertyAccessor;
import org.springframework.beans.NotWritablePropertyException;
import org.springframework.core.convert.ConversionService;
import org.springframework.data.document.mongodb.MongoPropertyDescriptors;
import org.springframework.data.document.mongodb.MongoPropertyDescriptors.MongoPropertyDescriptor;
import org.springframework.util.Assert;

/**
 * Custom Mongo specific {@link BeanWrapper} to allow access to bean properties via {@link MongoPropertyDescriptor}s.
 * 
 * @author Oliver Gierke
 */
class MongoBeanWrapper {

	private final ConfigurablePropertyAccessor accessor;
	private final MongoPropertyDescriptors descriptors;
	private final boolean fieldAccess;

	/**
	 * Creates a new {@link MongoBeanWrapper} for the given target object and {@link ConversionService}.
	 * 
	 * @param target
	 * @param conversionService
	 * @param fieldAccess
	 */
	public MongoBeanWrapper(Object target, ConversionService conversionService, boolean fieldAccess) {

		Assert.notNull(target);
		Assert.notNull(conversionService);

		this.fieldAccess = fieldAccess;
		this.accessor = fieldAccess ? forDirectFieldAccess(target) : forBeanPropertyAccess(target);
		this.accessor.setConversionService(conversionService);
		this.descriptors = new MongoPropertyDescriptors(target.getClass());
	}

	/**
	 * Returns all {@link MongoPropertyDescriptors.MongoPropertyDescriptor}s for the underlying target object.
	 * 
	 * @return
	 */
	public MongoPropertyDescriptors getDescriptors() {
		return this.descriptors;
	}

	/**
	 * Returns the value of the underlying object for the given property.
	 * 
	 * @param descriptor
	 * @return
	 */
	public Object getValue(MongoPropertyDescriptors.MongoPropertyDescriptor descriptor) {
		Assert.notNull(descriptor);
		return accessor.getPropertyValue(descriptor.getName());
	}

	/**
	 * Sets the property of the underlying object to the given value.
	 * 
	 * @param descriptor
	 * @param value
	 */
	public void setValue(MongoPropertyDescriptors.MongoPropertyDescriptor descriptor, Object value) {
		Assert.notNull(descriptor);
		try {
			accessor.setPropertyValue(descriptor.getName(), value);
		} catch (NotWritablePropertyException e) {
			if (!fieldAccess) {
				throw e;
			}
		}
	}
}