/*
 * Copyright 2018-2024 the original author or authors.
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

import java.util.ArrayList;
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
 * @author Mark Paluch
 * @since 2.1
 */
public class MongoJsonSchemaMapper implements JsonSchemaMapper {

	private static final String $JSON_SCHEMA = "$jsonSchema";
	private static final String REQUIRED_FIELD = "required";
	private static final String PROPERTIES_FIELD = "properties";
	private static final String ENUM_FIELD = "enum";

	private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
	private final MongoConverter converter;

	/**
	 * Create a new {@link MongoJsonSchemaMapper} facilitating the given {@link MongoConverter}.
	 *
	 * @param converter must not be {@literal null}.
	 */
	public MongoJsonSchemaMapper(MongoConverter converter) {

		Assert.notNull(converter, "Converter must not be null");

		this.converter = converter;
		this.mappingContext = converter.getMappingContext();
	}

	public Document mapSchema(Document jsonSchema, Class<?> type) {

		Assert.notNull(jsonSchema, "Schema must not be null");
		Assert.notNull(type, "Type must not be null Please consider Object.class");
		Assert.isTrue(jsonSchema.containsKey($JSON_SCHEMA),
				() -> String.format("Document does not contain $jsonSchema field; Found: %s", jsonSchema));

		if (Object.class.equals(type)) {
			return new Document(jsonSchema);
		}

		return new Document($JSON_SCHEMA,
				mapSchemaObject(mappingContext.getPersistentEntity(type), jsonSchema.get($JSON_SCHEMA, Document.class)));
	}

	@SuppressWarnings("unchecked")
	private Document mapSchemaObject(@Nullable PersistentEntity<?, MongoPersistentProperty> entity, Document source) {

		Document sink = new Document(source);

		if (source.containsKey(REQUIRED_FIELD)) {
			sink.replace(REQUIRED_FIELD, mapRequiredProperties(entity, source.get(REQUIRED_FIELD, Collection.class)));
		}

		if (source.containsKey(PROPERTIES_FIELD)) {
			sink.replace(PROPERTIES_FIELD, mapProperties(entity, source.get(PROPERTIES_FIELD, Document.class)));
		}

		mapEnumValuesIfNecessary(sink);

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

		return mapEnumValuesIfNecessary(sink);
	}

	private Document mapEnumValuesIfNecessary(Document source) {

		Document sink = new Document(source);
		if (source.containsKey(ENUM_FIELD)) {
			sink.replace(ENUM_FIELD, mapEnumValues(source.get(ENUM_FIELD, Iterable.class)));
		}
		return sink;
	}

	private List<Object> mapEnumValues(Iterable<?> values) {

		List<Object> converted = new ArrayList<>();
		for (Object val : values) {
			converted.add(converter.convertToMongoType(val));
		}
		return converted;
	}

	private String getFieldName(@Nullable PersistentEntity<?, MongoPersistentProperty> entity, String sourceField) {

		if (entity == null) {
			return sourceField;
		}

		MongoPersistentProperty property = entity.getPersistentProperty(sourceField);
		return property != null ? property.getFieldName() : sourceField;
	}
}
