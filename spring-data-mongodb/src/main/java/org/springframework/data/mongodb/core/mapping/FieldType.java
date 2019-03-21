/*
 * Copyright 2018-2019 the original author or authors.
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

import org.bson.types.ObjectId;

/**
 * Enumeration of field value types that can be used to represent a {@link org.bson.Document} field value. This
 * enumeration contains a subset of {@link org.bson.BsonType} that is supported by the mapping and conversion
 * components.
 * <p/>
 * Bson types are identified by a {@code byte} {@link #getBsonType() value}. This enumeration typically returns the
 * according bson type value except for {@link #IMPLICIT} which is a marker to derive the field type from a property.
 * 
 * @author Mark Paluch
 * @since 2.2
 * @see org.bson.BsonType
 */
public enum FieldType {

	/**
	 * Implicit type that is derived from the property value.
	 */
	IMPLICIT(-1, Object.class), STRING(2, String.class), OBJECT_ID(7, ObjectId.class);

	private final int bsonType;
	private final Class<?> javaClass;

	FieldType(int bsonType, Class<?> javaClass) {

		this.bsonType = bsonType;
		this.javaClass = javaClass;
	}

	/**
	 * Returns the BSON type identifier. Can be {@code -1} if {@link FieldType} maps to a synthetic Bson type.
	 * 
	 * @return the BSON type identifier. Can be {@code -1} if {@link FieldType} maps to a synthetic Bson type.
	 */
	public int getBsonType() {
		return bsonType;
	}

	/**
	 * Returns the Java class used to represent the type.
	 * 
	 * @return the Java class used to represent the type.
	 */
	public Class<?> getJavaClass() {
		return javaClass;
	}
}
