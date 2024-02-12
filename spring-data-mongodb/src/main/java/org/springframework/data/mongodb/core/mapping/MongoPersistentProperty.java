/*
 * Copyright 2011-2024 the original author or authors.
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
package org.springframework.data.mongodb.core.mapping;

import java.util.Collection;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

/**
 * MongoDB specific {@link org.springframework.data.mapping.PersistentProperty} extension.
 *
 * @author Oliver Gierke
 * @author Patryk Wasik
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Divya Srivastava
 */
public interface MongoPersistentProperty extends PersistentProperty<MongoPersistentProperty> {

	/**
	 * Returns the name of the field a property is persisted to.
	 *
	 * @return
	 */
	String getFieldName();

	/**
	 * Returns whether the property uses an annotated field name through {@link Field}.
	 *
	 * @return
	 */
	boolean hasExplicitFieldName();

	/**
	 * Returns the {@link Class Java FieldType} of the field a property is persisted to.
	 *
	 * @return
	 * @since 2.2
	 * @see FieldType
	 */
	Class<?> getFieldType();

	/**
	 * Returns the order of the field if defined. Will return -1 if undefined.
	 *
	 * @return
	 */
	int getFieldOrder();

	/**
	 * Returns whether the property should be written to the database if its value is {@literal null}.
	 *
	 * @return
	 * @since 3.3
	 * @see Field.Write
	 */
	boolean writeNullValues();

	/**
	 * Returns whether the property is a {@link com.mongodb.DBRef}. If this returns {@literal true} you can expect
	 * {@link #getDBRef()} to return an non-{@literal null} value.
	 *
	 * @return
	 */
	boolean isDbReference();

	/**
	 * Returns whether the property is a {@link DocumentReference}. If this returns {@literal true} you can expect
	 * {@link #getDocumentReference()} to return an non-{@literal null} value.
	 *
	 * @return
	 * @since 3.3
	 */
	boolean isDocumentReference();

	/**
	 * Returns whether the property is explicitly marked as an identifier property of the owning {@link PersistentEntity}.
	 * A property is an explicit id property if it is annotated with @see {@link Id}.
	 *
	 * @return
	 */
	boolean isExplicitIdProperty();

	/**
	 * Returns true whether the property indicates the documents language either by having a {@link #getFieldName()} equal
	 * to {@literal language} or being annotated with {@link Language}.
	 *
	 * @return
	 * @since 1.6
	 */
	boolean isLanguageProperty();

	/**
	 * Returns true when property being annotated with {@link Language}.
	 *
	 * @return
	 * @since 1.6.1
	 */
	boolean isExplicitLanguageProperty();

	/**
	 * Returns whether the property holds the documents score calculated by text search. <br/>
	 * It's marked with {@link TextScore}.
	 *
	 * @return
	 * @since 1.6
	 */
	boolean isTextScoreProperty();

	/**
	 * Returns the {@link DBRef} if the property is a reference.
	 *
	 * @see #isDbReference()
	 * @return
	 */
	@Nullable
	DBRef getDBRef();

	/**
	 * Returns the {@link DocumentReference} if the property is a reference.
	 *
	 * @see #isDocumentReference()
	 * @return {@literal null} if not present.
	 * @since 3.3
	 */
	@Nullable
	DocumentReference getDocumentReference();

	/**
	 * Returns whether property access shall be used for reading the property value. This means it will use the getter
	 * instead of field access.
	 *
	 * @return
	 */
	boolean usePropertyAccess();

	/**
	 * @return {@literal true} if the property defines an explicit {@link Field#targetType() target type}.
	 * @since 2.2
	 */
	default boolean hasExplicitWriteTarget() {

		Field field = findAnnotation(Field.class);
		return field != null && !FieldType.IMPLICIT.equals(field.targetType());
	}

	/**
	 * @return {@literal true} if the property should be unwrapped.
	 * @since 3.2
	 */
	default boolean isUnwrapped() {
		return isEntity() && isAnnotationPresent(Unwrapped.class);
	}

	/**
	 * @return the resolved encryption keyIds if applicable. An empty {@link Collection} if no keyIds specified.
	 *         {@literal null} no {@link Encrypted} annotation found.
	 * @since 3.3
	 */
	Collection<Object> getEncryptionKeyIds();

	/**
	 * @return the {@link MongoField} representing the raw field to read/write in a MongoDB document.
	 * @since 4.2
	 */
	MongoField getMongoField();

	/**
	 * Simple {@link Converter} implementation to transform a {@link MongoPersistentProperty} into its field name.
	 *
	 * @author Oliver Gierke
	 */
	enum PropertyToFieldNameConverter implements Converter<MongoPersistentProperty, String> {

		INSTANCE;

		@NonNull
		@Override
		public String convert(MongoPersistentProperty source) {
			if (!source.isUnwrapped()) {
				return source.getFieldName();
			}
			return "";
		}
	}
}
