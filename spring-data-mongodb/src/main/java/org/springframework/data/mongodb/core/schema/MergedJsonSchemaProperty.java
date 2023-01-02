/*
 * Copyright 2022-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.schema;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import org.bson.Document;
import org.springframework.data.mongodb.core.schema.MongoJsonSchema.ConflictResolutionFunction;

/**
 * {@link JsonSchemaProperty} implementation that is capable of combining multiple properties with different values into
 * a single one.
 *
 * @author Christoph Strobl
 * @since 3.4
 */
class MergedJsonSchemaProperty implements JsonSchemaProperty {

	private final Iterable<JsonSchemaProperty> properties;
	private final BiFunction<Map<String, Object>, Map<String, Object>, Document> mergeFunction;

	MergedJsonSchemaProperty(Iterable<JsonSchemaProperty> properties) {
		this(properties, (k, a, b) -> {
			throw new IllegalStateException(
					String.format("Error resolving conflict for '%s'; No conflict resolution function defined", k));
		});
	}

	MergedJsonSchemaProperty(Iterable<JsonSchemaProperty> properties,
			ConflictResolutionFunction conflictResolutionFunction) {
		this(properties, new TypeUnifyingMergeFunction(conflictResolutionFunction));
	}

	MergedJsonSchemaProperty(Iterable<JsonSchemaProperty> properties,
			BiFunction<Map<String, Object>, Map<String, Object>, Document> mergeFunction) {

		this.properties = properties;
		this.mergeFunction = mergeFunction;
	}

	@Override
	public Set<Type> getTypes() {
		return Collections.emptySet();
	}

	@Override
	public Document toDocument() {

		Document document = new Document();

		for (JsonSchemaProperty property : properties) {
			document = mergeFunction.apply(document, property.toDocument());
		}
		return document;
	}

	@Override
	public String getIdentifier() {
		return properties.iterator().next().getIdentifier();
	}
}
