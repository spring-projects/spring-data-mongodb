/*
 * Copyright 2011-2014 the original author or authors.
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

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PersistentProperty;

/**
 * MongoDB specific {@link org.springframework.data.mapping.PersistentProperty} extension.
 * 
 * @author Oliver Gierke
 * @author Patryk Wasik
 * @author Thomas Darimont
 * @author Christoph Strobl
 */
public interface MongoPersistentProperty extends PersistentProperty<MongoPersistentProperty> {

	/**
	 * Returns the name of the field a property is persisted to.
	 * 
	 * @return
	 */
	String getFieldName();

	/**
	 * Returns the order of the field if defined. Will return -1 if undefined.
	 * 
	 * @return
	 */
	int getFieldOrder();

	/**
	 * Returns whether the propert is a {@link com.mongodb.DBRef}. If this returns {@literal true} you can expect
	 * {@link #getDBRef()} to return an non-{@literal null} value.
	 * 
	 * @return
	 */
	boolean isDbReference();

	/**
	 * Returns whether the property is explicitly marked as an identifier property of the owning {@link PersistentEntity}.
	 * A property is an explicit id property if it is annotated with @see {@link Id}.
	 * 
	 * @return
	 */
	boolean isExplicitIdProperty();

	/**
	 * Returns whether the property indicates the documents language either by having a {@link #getFieldName()} equal to
	 * {@literal language} or being annotated with {@link Language}.
	 * 
	 * @return
	 * @since 1.6
	 */
	boolean isLanguageProperty();

	/**
	 * Returns whether the property holds the documents score calculated by text search. <br/>
	 * It's marked with {@link TextScore}.
	 * 
	 * @return
	 * @since 1.6
	 */
	boolean isTextScoreProperty();

	/**
	 * Returns wheter the property is calculated eiter internally or on the server and therefore must not be written when
	 * saved.
	 * 
	 * @return
	 * @since 1.6
	 */
	boolean isCalculatedProperty();

	/**
	 * Returns the {@link DBRef} if the property is a reference.
	 * 
	 * @see #isDbReference()
	 * @return
	 */
	DBRef getDBRef();

	/**
	 * Simple {@link Converter} implementation to transform a {@link MongoPersistentProperty} into its field name.
	 * 
	 * @author Oliver Gierke
	 */
	public enum PropertyToFieldNameConverter implements Converter<MongoPersistentProperty, String> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		public String convert(MongoPersistentProperty source) {
			return source.getFieldName();
		}
	}

	/**
	 * Returns whether property access shall be used for reading the property value. This means it will use the getter
	 * instead of field access.
	 * 
	 * @return
	 */
	boolean usePropertyAccess();
}
