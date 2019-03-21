/*
 * Copyright 2014-2019 the original author or authors.
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
package org.springframework.data.mongodb.core.index;

import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;

/**
 * {@link IndexResolver} finds those {@link IndexDefinition}s to be created for a given class.
 *
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Mark Paluch
 * @since 1.5
 */
public interface IndexResolver {

	/**
	 * Creates a new {@link IndexResolver} given {@link MongoMappingContext}.
	 *
	 * @param mappingContext must not be {@literal null}.
	 * @return the new {@link IndexResolver}.
	 * @since 2.2
	 */
	static IndexResolver create(MongoMappingContext mappingContext) {

		Assert.notNull(mappingContext, "MongoMappingContext must not be null!");

		return new MongoPersistentEntityIndexResolver(mappingContext);
	}

	/**
	 * Find and create {@link IndexDefinition}s for properties of given {@link TypeInformation}. {@link IndexDefinition}s
	 * are created for properties and types with {@link Indexed}, {@link CompoundIndexes} or {@link GeoSpatialIndexed}.
	 *
	 * @param typeInformation
	 * @return Empty {@link Iterable} in case no {@link IndexDefinition} could be resolved for type.
	 */
	Iterable<? extends IndexDefinition> resolveIndexFor(TypeInformation<?> typeInformation);

	/**
	 * Find and create {@link IndexDefinition}s for properties of given {@link TypeInformation}. {@link IndexDefinition}s
	 * are created for properties and types with {@link Indexed}, {@link CompoundIndexes} or {@link GeoSpatialIndexed}.
	 *
	 * @param entityType
	 * @return Empty {@link Iterable} in case no {@link IndexDefinition} could be resolved for type.
	 * @see 2.2
	 */
	default Iterable<? extends IndexDefinition> resolveIndexFor(Class<?> entityType) {
		return resolveIndexFor(ClassTypeInformation.from(entityType));
	}

}
