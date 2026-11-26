/*
 * Copyright 2026-present the original author or authors.
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

import org.bson.Document;
import org.bson.types.ObjectId;
import org.jspecify.annotations.Nullable;

/**
 * Strategy to determine the target type of {@literal _id} values where none has been defined explicitly. This applies
 * to {@literal id} properties without {@link Field &#64;Field} or {@link MongoId &#64;MongoId} annotation and to
 * {@literal _id} values without any mapping information, e.g. when operating on plain {@link Document documents}.
 * Explicitly annotated {@literal id} properties are not subject to this strategy.
 * <p>
 * Implementations must not call {@link MongoPersistentProperty#getFieldType()} on the given property, as the field
 * type is derived from this strategy.
 *
 * @author Christoph Strobl
 * @since 5.3
 * @see MongoId
 * @see MongoMappingContext#setIdTargetTypeStrategy(IdTargetTypeStrategy)
 * @see org.springframework.data.mongodb.core.convert.MongoCustomConversions.MongoConverterConfigurationAdapter#idTargetTypeStrategy(IdTargetTypeStrategy)
 */
@FunctionalInterface
public interface IdTargetTypeStrategy {

	/**
	 * Targets {@link ObjectId} which converts {@link String} values that represent a valid {@link ObjectId#isValid(String)
	 * ObjectId hex String}. This is the default.
	 */
	IdTargetTypeStrategy OBJECT_ID = IdTargetTypeStrategies.OBJECT_ID;

	/**
	 * Targets the property type, or {@link Document} for entity types, equivalent to {@link MongoId &#64;MongoId} with
	 * {@link FieldType#IMPLICIT}. Values without mapping information are not converted to a different {@literal id}
	 * type.
	 */
	IdTargetTypeStrategy IMPLICIT = IdTargetTypeStrategies.IMPLICIT;

	/**
	 * Determine the target type for {@literal _id} values.
	 *
	 * @param property the {@literal id} property. Can be {@literal null} if no mapping information is available.
	 * @return the target type. {@link Object Object.class} to retain the value type. Never {@literal null}.
	 */
	Class<?> getIdTargetType(@Nullable MongoPersistentProperty property);

}
