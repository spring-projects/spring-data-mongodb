/*
 * Copyright 2018 the original author or authors.
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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.core.annotation.AliasFor;
import org.springframework.data.annotation.Id;

/**
 * {@link MongoId} represents a MongoDB specific {@link Id} annotation that allows customizing {@literal id} conversion.
 * Id properties use {@link org.springframework.data.mongodb.core.mapping.FieldType#IMPLICIT} as the default
 * {@literal id's} target type. This means that the actual property value is used. No conversion attempts to any other
 * type are made. <br />
 * In contrast to {@link Id &#64;Id}, {@link String} {@literal id's} are stored as the such even when the actual value
 * represents a valid {@link org.bson.types.ObjectId#isValid(String) ObjectId hex String}. To trigger {@link String} to
 * {@link org.bson.types.ObjectId} conversion use {@link MongoId#targetType() &#64;MongoId(FieldType.OBJECT_ID)}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.2
 */
@Id
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD, ElementType.METHOD, ElementType.ANNOTATION_TYPE })
public @interface MongoId {

	/**
	 * @return the preferred id type.
	 * @see #targetType()
	 */
	@AliasFor("targetType")
	FieldType value() default FieldType.IMPLICIT;

	/**
	 * Get the preferred {@literal _id} type to be used. Defaults to {@link FieldType#IMPLICIT} which uses the property's
	 * type. If defined different, the given value is attempted to be converted into the desired target type via
	 * {@link org.springframework.data.mongodb.core.convert.MongoConverter#convertId(Object, Class)}.
	 *
	 * @return the preferred {@literal id} type. {@link FieldType#IMPLICIT} by default.
	 */
	@AliasFor("value")
	FieldType targetType() default FieldType.IMPLICIT;

}
