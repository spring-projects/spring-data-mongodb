/*
 * Copyright 2017 the original author or authors.
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

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.bson.Document;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.schema.JsonSchemaObject.Type;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * {@link JsonSchemaMapper} implementation using the conversion and mapping infrastructure for mapping fields to the
 * provided domain type.
 *
 * @author Christoph Strobl
 * @since 2.1
 */
public class MongoJsonSchemaMapper implements JsonSchemaMapper {

	private static final String $JSON_SCHEMA = "$jsonSchema";
	private static final String REQUIRED_FIELD = "required";
	private static final String PROPERTIES_FIELD = "properties";

	private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;

	/**
	 * Create a new {@link MongoJsonSchemaMapper} facilitating the given {@link MongoConverter}.
	 *
	 * @param converter must not be {@literal null}.
	 */
	public MongoJsonSchemaMapper(MongoConverter converter) {
		this.mappingContext = converter.getMappingContext();
	}

	/*
	 * (non-Javadoc) 
	 * @see org.springframework.data.mongodb.core.convert.JsonSchemaMapper#mapSchema(org.springframework.data.mongodb.core.schema.MongoJsonSchema, java.lang.Class)
	 */
	public Document mapSchema(Document jsonSchema, Class<?> type) {

		Assert.notNull(jsonSchema, "Schema must not be null!");
		Assert.notNull(type, "Type must not be null! Please consider Object.class.");
		Assert.isTrue(jsonSchema.containsKey($JSON_SCHEMA),
				() -> String.format("Document does not contain $jsonSchema field. Found %s.", jsonSchema));

		if (Object.class.equals(type)) {
			return new Document(jsonSchema);
		}

		return new Document($JSON_SCHEMA,
				mapSchemaObject(mappingContext.getPersistentEntity(type), jsonSchema.get($JSON_SCHEMA, Document.class)));
	}

	private Document mapSchemaObject(@Nullable PersistentEntity entity, Document source) {

		Document sink = new Document(source);

		if (source.containsKey(REQUIRED_FIELD)) {
			sink.replace(REQUIRED_FIELD, mapRequiredProperties(entity, source.get(REQUIRED_FIELD, Collection.class)));
		}

		if (source.containsKey(PROPERTIES_FIELD)) {
			sink.replace(PROPERTIES_FIELD, mapProperties(entity, source.get(PROPERTIES_FIELD, Document.class)));
		}
		return sink;
	}

	private Document mapProperties(@Nullable PersistentEntity<?, MongoPersistentProperty> entity, Document source) {

		Document sink = new Document();
		for (String fieldName : source.keySet()) {

			String mappedFieldName = getFieldName(entity, fieldName);
			Document mappedProperty = mapProperty(entity, fieldName, source.get(fieldName, Document.class));

			sink.append(mappedFieldName, mappedProperty);
		}
		return sink;
	}

	private List<String> mapRequiredProperties(@Nullable PersistentEntity<?, MongoPersistentProperty> entity,
			Collection<String> sourceFields) {

		return sourceFields.stream() ///
				.map(fieldName -> getFieldName(entity, fieldName)) //
				.collect(Collectors.toList());
	}

	private Document mapProperty(@Nullable PersistentEntity<?, MongoPersistentProperty> entity, String sourceFieldName,
			Document source) {

		Document sink = new Document(source);

		if (entity != null && sink.containsKey(Type.objectType().representation())) {

			MongoPersistentProperty property = entity.getPersistentProperty(sourceFieldName);
			if (property != null && property.isEntity()) {
				sink = mapSchemaObject(mappingContext.getPersistentEntity(property.getActualType()), source);
			}
		}

		return sink;
	}

	private String getFieldName(@Nullable PersistentEntity<?, MongoPersistentProperty> entity, String sourceField) {

		if (entity == null) {
			return sourceField;
		}

		MongoPersistentProperty property = entity.getPersistentProperty(sourceField);
		return property != null ? property.getFieldName() : sourceField;
	}
}
