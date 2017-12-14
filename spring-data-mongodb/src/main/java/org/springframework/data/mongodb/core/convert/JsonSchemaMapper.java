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
package org.springframework.data.mongodb.core.convert;

import org.bson.Document;

/**
 * {@link JsonSchemaMapper} allows mapping a given {@link Document} containing a {@literal $jsonSchema} to the fields of
 * a given domain type. The mapping considers {@link org.springframework.data.mongodb.core.mapping.Field} annotations
 * and other Spring Data specifics.
 *
 * @author Christoph Strobl
 * @since 2.1
 */
public interface JsonSchemaMapper {

	/**
	 * Map the {@literal required} and {@literal properties} fields the given {@link Document} containing the
	 * {@literal $jsonSchema} against the given domain type. <br />
	 * The source document remains untouched, fields that do not require mapping are simply copied over to the mapped
	 * instance.
	 *
	 * @param jsonSchema the {@link Document} holding the raw schema representation. Must not be {@literal null}.
	 * @param type the target type to map against. Must not be {@literal null}.
	 * @return a <strong>new</strong> {@link Document} containing the mapped {@literal $jsonSchema} never {@literal null}.
	 */
	Document mapSchema(Document jsonSchema, Class<?> type);
}
