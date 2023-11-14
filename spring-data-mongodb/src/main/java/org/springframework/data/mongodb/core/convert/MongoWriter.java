/*
 * Copyright 2010-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.convert;

import org.bson.conversions.Bson;
import org.springframework.data.convert.EntityWriter;
import org.springframework.data.mongodb.core.mapping.DocumentPointer;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;

import com.mongodb.DBRef;

/**
 * A MongoWriter is responsible for converting an object of type T to the native MongoDB representation Document.
 *
 * @param <T> the type of the object to convert to a Document
 * @author Mark Pollack
 * @author Thomas Risberg
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
public interface MongoWriter<T> extends EntityWriter<T, Bson> {

	/**
	 * Converts the given object into one Mongo will be able to store natively. If the given object can already be stored
	 * as is, no conversion will happen.
	 *
	 * @param obj can be {@literal null}.
	 * @return
	 */
	@Nullable
	default Object convertToMongoType(@Nullable Object obj) {
		return convertToMongoType(obj, (TypeInformation<?>) null);
	}

	/**
	 * Converts the given object into one Mongo will be able to store natively but retains the type information in case
	 * the given {@link TypeInformation} differs from the given object type.
	 *
	 * @param obj can be {@literal null}.
	 * @param typeInformation can be {@literal null}.
	 * @return
	 */
	@Nullable
	Object convertToMongoType(@Nullable Object obj, @Nullable TypeInformation<?> typeInformation);

	default Object convertToMongoType(@Nullable Object obj, MongoPersistentEntity<?> entity) {
		return convertToMongoType(obj, entity.getTypeInformation());
	}

	/**
	 * Creates a {@link DBRef} to refer to the given object.
	 *
	 * @param object the object to create a {@link DBRef} to link to. The object's type has to carry an id attribute.
	 * @param referringProperty the client-side property referring to the object which might carry additional metadata for
	 *          the {@link DBRef} object to create. Can be {@literal null}.
	 * @return will never be {@literal null}.
	 */
	DBRef toDBRef(Object object, @Nullable MongoPersistentProperty referringProperty);

	/**
	 * Creates a the {@link DocumentPointer} representing the link to another entity.
	 *
	 * @param source the object to create a document link to.
	 * @param referringProperty the client-side property referring to the object which might carry additional metadata for
	 *          the {@link DBRef} object to create. Can be {@literal null}.
	 * @return will never be {@literal null}.
	 * @since 3.3
	 */
	default DocumentPointer<?> toDocumentPointer(Object source, @Nullable MongoPersistentProperty referringProperty) {
		return () -> toDBRef(source, referringProperty);
	}
}
