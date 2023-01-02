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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import org.bson.Document;

/**
 * {@link MongoJsonSchema} implementation that is capable of merging properties from different schemas into a single
 * one.
 *
 * @author Christoph Strobl
 * @since 3.4
 */
class MergedJsonSchema implements MongoJsonSchema {

	private final List<MongoJsonSchema> schemaList;
	private final BiFunction<Map<String, Object>, Map<String, Object>, Document> mergeFunction;

	MergedJsonSchema(List<MongoJsonSchema> schemaList, ConflictResolutionFunction conflictResolutionFunction) {
		this(schemaList, new TypeUnifyingMergeFunction(conflictResolutionFunction));
	}

	MergedJsonSchema(List<MongoJsonSchema> schemaList,
			BiFunction<Map<String, Object>, Map<String, Object>, Document> mergeFunction) {

		this.schemaList = new ArrayList<>(schemaList);
		this.mergeFunction = mergeFunction;
	}

	@Override
	public MongoJsonSchema mergeWith(Collection<MongoJsonSchema> sources) {

		schemaList.addAll(sources);
		return this;
	}

	@Override
	public Document schemaDocument() {

		Document targetSchema = new Document();
		for (MongoJsonSchema schema : schemaList) {
			targetSchema = mergeFunction.apply(targetSchema, schema.schemaDocument());
		}

		return targetSchema;
	}
}
