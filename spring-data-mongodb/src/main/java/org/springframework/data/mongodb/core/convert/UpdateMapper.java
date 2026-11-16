/*
 * Copyright 2013-present the original author or authors.
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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.jspecify.annotations.Nullable;

import org.springframework.data.convert.PropertyValueConverter;
import org.springframework.data.convert.ValueConversionContext;
import org.springframework.data.core.TypeInformation;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Order;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mongodb.core.convert.MongoConversionContext.WriteOperatorContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.mapping.path.BoundMongoPath;
import org.springframework.data.mongodb.core.mapping.path.MongoPath;
import org.springframework.data.mongodb.core.mapping.path.MongoPath.FilteredPositional;
import org.springframework.data.mongodb.core.mapping.path.MongoPath.Token;
import org.springframework.data.mongodb.core.mapping.path.MongoPathBinder;
import org.springframework.data.mongodb.core.mapping.path.MongoPath.PathType;
import org.springframework.data.mongodb.core.mapping.path.Scope;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update.Modifier;
import org.springframework.data.mongodb.core.query.Update.Modifiers;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.data.mongodb.core.query.UpdateDefinition.ArrayFilter;
import org.springframework.util.ObjectUtils;

/**
 * A subclass of {@link QueryMapper} that retains type information on the mongo types.
 *
 * @author Thomas Darimont
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class UpdateMapper extends QueryMapper {

	private static final Set<String> LOGICAL_OPERATORS = Set.of("$and", "$or", "$nor");

	private final MongoConverter converter;

	/**
	 * Creates a new {@link UpdateMapper} using the given {@link MongoConverter}.
	 *
	 * @param converter must not be {@literal null}.
	 */
	public UpdateMapper(MongoConverter converter) {

		super(converter);
		this.converter = converter;
	}

	@Override
	public Document getMappedObject(Bson query, @Nullable MongoPersistentEntity<?> entity) {

		Document document = super.getMappedObject(query, entity);

		boolean hasOperators = false;
		boolean hasFields = false;

		Document set = null;
		for (String s : document.keySet()) {
			if (s.startsWith("$")) {

				if (s.equals("$set")) {
					set = document.get(s, Document.class);
				}
				hasOperators = true;
			} else {
				hasFields = true;
			}
		}

		if (hasOperators && hasFields) {

			Document updateObject = new Document();
			Document fieldsToSet = set == null ? new Document() : set;

			for (String s : document.keySet()) {
				if (s.startsWith("$")) {
					updateObject.put(s, document.get(s));
				} else {
					fieldsToSet.put(s, document.get(s));
				}
			}
			updateObject.put("$set", fieldsToSet);

			return updateObject;
		}
		return document;
	}

	/**
	 * Returns {@literal true} if the given {@link Document} is an update object that uses update operators.
	 *
	 * @param updateObj can be {@literal null}.
	 * @return {@literal true} if the given {@link Document} is an update object.
	 */
	public static boolean isUpdateObject(@Nullable Document updateObj) {

		if (updateObj == null) {
			return false;
		}

		for (String s : updateObj.keySet()) {
			if (s.startsWith("$")) {
				return true;
			}
		}

		return false;
	}

	/**
	 * Map the {@link UpdateDefinition#getArrayFilters() array filters} of the given {@link UpdateDefinition}. Filter keys
	 * are resolved against the element type of the array the {@code $[<identifier>]} refers to in the update. Filters
	 * using identifiers that cannot be resolved unambiguously are retained as-is.
	 *
	 * @param update must not be {@literal null}.
	 * @param entity can be {@literal null}.
	 * @return the mapped array filters.
	 * @since 5.3
	 */
	public List<Document> getMappedArrayFilters(UpdateDefinition update, @Nullable MongoPersistentEntity<?> entity) {

		List<Document> filters = new ArrayList<>(update.getArrayFilters().size());
		for (ArrayFilter arrayFilter : update.getArrayFilters()) {
			filters.add(arrayFilter.asDocument());
		}

		if (entity == null || filters.isEmpty()) {
			return filters;
		}

		Map<String, MetadataBackedField> identifiers = getArrayFilterIdentifiers(update.getUpdateObject(), entity);
		if (identifiers.isEmpty()) {
			return filters;
		}

		List<Document> mapped = new ArrayList<>(filters.size());
		for (Document filter : filters) {
			mapped.add(getMappedArrayFilter(filter, identifiers));
		}
		return mapped;
	}

	private Map<String, MetadataBackedField> getArrayFilterIdentifiers(Document updateObject,
			MongoPersistentEntity<?> entity) {

		Map<String, BoundMongoPath> paths = new LinkedHashMap<>();
		Set<String> ambiguous = new HashSet<>();

		for (Entry<String, Object> entry : updateObject.entrySet()) {
			if (isKeyword(entry.getKey()) && entry.getValue() instanceof Document operation) {
				for (String key : operation.keySet()) {
					collectArrayFilterIdentifiers(key, entity, paths, ambiguous);
				}
			} else {
				collectArrayFilterIdentifiers(entry.getKey(), entity, paths, ambiguous);
			}
		}

		if (!ambiguous.isEmpty() && LOGGER.isDebugEnabled()) {
			LOGGER.debug("Not mapping array filters for ambiguous identifiers %s".formatted(ambiguous));
		}

		Map<String, MetadataBackedField> identifiers = new LinkedHashMap<>(paths.size());
		paths.forEach((identifier, path) -> {
			if (!ambiguous.contains(identifier)) {
				identifiers.put(identifier,
						MetadataBackedField.arrayFilterElement(identifier, path, entity, getMappingContext()));
			}
		});
		return identifiers;
	}

	private void collectArrayFilterIdentifiers(String key, MongoPersistentEntity<?> entity,
			Map<String, BoundMongoPath> paths, Set<String> ambiguous) {

		MongoPath path;
		try {
			path = MongoPath.parse(key, PathType.UPDATE);
		} catch (IllegalArgumentException ex) {
			return;
		}

		List<Token> tokens = path.tokens();
		for (int i = 0; i < tokens.size(); i++) {

			if (!(tokens.get(i) instanceof FilteredPositional positional)) {
				continue;
			}

			String identifier = positional.identifier();
			BoundMongoPath elementPath = bindArrayFilterElement(tokens.subList(0, i + 1), entity);

			if (elementPath == null) {
				ambiguous.add(identifier);
				continue;
			}

			BoundMongoPath existing = paths.putIfAbsent(identifier, elementPath);
			if (existing != null && !ObjectUtils.nullSafeEquals(existing.targetType(), elementPath.targetType())) {
				ambiguous.add(identifier);
			}
		}
	}

	private @Nullable BoundMongoPath bindArrayFilterElement(List<Token> tokens, MongoPersistentEntity<?> entity) {

		String path = tokens.stream().map(Token::text).collect(Collectors.joining("."));

		try {
			BoundMongoPath bound = MongoPathBinder.of(getMappingContext()).bind(path, PathType.UPDATE, Scope.of(entity));
			return bound.isResolved() && bound.association() == null ? bound : null;
		} catch (MappingException ex) {
			return null;
		}
	}

	private Document getMappedArrayFilter(Document filter, Map<String, MetadataBackedField> identifiers) {

		Document result = new Document();

		for (Entry<String, Object> entry : filter.entrySet()) {

			String key = entry.getKey();
			Object value = entry.getValue();

			if (LOGICAL_OPERATORS.contains(key) && value instanceof Iterable<?> conditions) {

				List<Object> mapped = new ArrayList<>();
				for (Object condition : conditions) {
					mapped.add(condition instanceof Document document ? getMappedArrayFilter(document, identifiers) : condition);
				}
				result.put(key, mapped);
				continue;
			}

			Field field = getArrayFilterField(key, identifiers);
			if (field == null) {
				result.put(key, value);
				continue;
			}

			Entry<String, @Nullable Object> mapped = super.getMappedObjectForField(field, value);
			result.put(mapped.getKey(), mapped.getValue());
		}

		return result;
	}

	private static @Nullable Field getArrayFilterField(String key, Map<String, MetadataBackedField> identifiers) {

		int dot = key.indexOf('.');
		String identifier = dot == -1 ? key : key.substring(0, dot);

		MetadataBackedField element = identifiers.get(identifier);
		if (element == null || dot == -1) {
			return element;
		}

		return element.nested(key.substring(dot + 1), identifier);
	}

	@Override
	protected PathType getPathType() {
		return PathType.UPDATE;
	}

	/**
	 * Converts the given source object to a mongo type retaining the original type information of the source type on the
	 * mongo type.
	 *
	 * @see org.springframework.data.mongodb.core.convert.QueryMapper#delegateConvertToMongoType(java.lang.Object,
	 *      org.springframework.data.mongodb.core.mapping.MongoPersistentEntity)
	 */
	@Override
	protected @Nullable Object delegateConvertToMongoType(Object source, @Nullable MongoPersistentEntity<?> entity) {

		if (entity != null && entity.isUnwrapped()) {
			return converter.convertToMongoType(source, entity);
		}

		return converter.convertToMongoType(source,
				entity == null ? TypeInformation.OBJECT : getTypeHintForEntity(source, entity));
	}

	@Override
	@SuppressWarnings("NullAway")
	protected Entry<String, @Nullable Object> getMappedObjectForField(Field field, @Nullable Object rawValue) {

		if (isDocument(rawValue)) {

			if (field.isMap()) { // unwrap to preserve field type
				return createMapEntry(field,
						convertSimpleOrDocument(new LinkedHashMap<>((Document) rawValue), field.getPropertyEntity()));
			}
			return createMapEntry(field, getMappedNestedObject((Document) rawValue, field));
		}

		if (isQuery(rawValue)) {
			return createMapEntry(field,
					super.getMappedObject(((Query) rawValue).getQueryObject(), field.getPropertyEntity()));
		}

		if (isUpdateModifier(rawValue)) {
			return getMappedUpdateModifier(field, rawValue);
		}

		return super.getMappedObjectForField(field, rawValue);
	}

	protected @Nullable Object convertValueWithConversionContext(Field documentField, @Nullable Object sourceValue, @Nullable Object value,
		PropertyValueConverter<Object, Object, ValueConversionContext<MongoPersistentProperty>> valueConverter,
		MongoConversionContext conversionContext) {

		return super.convertValueWithConversionContext(documentField, sourceValue, value, valueConverter, conversionContext.forOperator(new WriteOperatorContext(documentField.name)));
	}

	private Entry<String, Object> getMappedUpdateModifier(Field field, Object rawValue) {
		Object value;

		if (rawValue instanceof Modifier modifier) {

			value = getMappedValue(field, modifier);

		} else if (rawValue instanceof Modifiers modifiers) {

			Document modificationOperations = new Document();

			for (Modifier modifier : modifiers.getModifiers()) {
				modificationOperations.putAll(getMappedValue(field, modifier));
			}

			value = modificationOperations;
		} else {
			throw new IllegalArgumentException(String.format("Unable to map value of type '%s'", rawValue.getClass()));
		}

		return createMapEntry(field, value);
	}

	@Override
	protected boolean isAssociationConversionNecessary(Field documentField, @Nullable Object value) {
		return super.isAssociationConversionNecessary(documentField, value) || documentField.containsAssociation();
	}

	private boolean isUpdateModifier(@Nullable Object value) {
		return value instanceof Modifier || value instanceof Modifiers;
	}

	private boolean isQuery(@Nullable Object value) {
		return value instanceof Query;
	}

	private @Nullable Document getMappedValue(@Nullable Field field, Modifier modifier) {
		return new Document(modifier.getKey(), getMappedModifier(field, modifier));
	}

	@SuppressWarnings("NullAway")
	private @Nullable Object getMappedModifier(@Nullable Field field, Modifier modifier) {

		Object value = modifier.getValue();

		if (value instanceof Sort) {

			Document sortObject = getSortObject((Sort) value);
			return field == null || field.getPropertyEntity() == null ? sortObject
					: getMappedSort(sortObject, field.getPropertyEntity());
		}

		if (field != null && isAssociationConversionNecessary(field, value)) {
			if (ObjectUtils.isArray(value) || value instanceof Collection) {
				List<Object> targetPointers = new ArrayList<>();
				for (Object val : converter.getConversionService().convert(value, List.class)) {
					targetPointers.add(getMappedValue(field, val));
				}
				return targetPointers;
			}
			return super.getMappedValue(field, value);
		}

		TypeInformation<?> typeHint = field == null ? TypeInformation.OBJECT : field.getTypeHint();
		return converter.convertToMongoType(value, typeHint);
	}

	private TypeInformation<?> getTypeHintForEntity(@Nullable Object source, MongoPersistentEntity<?> entity) {

		TypeInformation<?> info = entity.getTypeInformation();
		Class<?> type = info.getRequiredActualType().getType();

		if (source == null || type.isInterface() || java.lang.reflect.Modifier.isAbstract(type.getModifiers())) {
			return info;
		}

		if (source instanceof Collection) {
			return NESTED_DOCUMENT;
		}

		if (!type.equals(source.getClass())) {
			return info;
		}

		return NESTED_DOCUMENT;
	}

	private static Document getSortObject(Sort sort) {

		Document document = new Document();

		for (Order order : sort) {
			document.put(order.getProperty(), order.isAscending() ? 1 : -1);
		}

		return document;
	}
}
