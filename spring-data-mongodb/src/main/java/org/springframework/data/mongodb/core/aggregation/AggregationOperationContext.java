/*
 * Copyright 2013 the original author or authors.
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
package org.springframework.data.mongodb.core.aggregation;

import org.bson.Document;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.FieldReference;

/**
 * The context for an {@link AggregationOperation}.
 * 
 * @author Oliver Gierke
 * @since 1.3
 */
public interface AggregationOperationContext {

	/**
	 * Returns the mapped {@link Document}, potentially converting the source considering mapping metadata etc.
	 * 
	 * @param dbObject will never be {@literal null}.
	 * @return must not be {@literal null}.
	 */
	Document getMappedObject(Document dbObject);

	/**
	 * Returns a {@link FieldReference} for the given field or {@literal null} if the context does not expose the given
	 * field.
	 * 
	 * @param field must not be {@literal null}.
	 * @return
	 */
	FieldReference getReference(Field field);

	/**
	 * Returns the {@link FieldReference} for the field with the given name or {@literal null} if the context does not
	 * expose a field with the given name.
	 * 
	 * @param name must not be {@literal null} or empty.
	 * @return
	 */
	FieldReference getReference(String name);
}
