/*
 * Copyright 2021-2023 the original author or authors.
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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.WeakHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.core.convert.ConversionService;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.annotation.Reference;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PersistentPropertyPathAccessor;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.BeanWrapperPropertyAccessorFactory;
import org.springframework.data.mongodb.core.mapping.DocumentPointer;
import org.springframework.data.mongodb.core.mapping.FieldName;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;

/**
 * Internal API to construct {@link DocumentPointer} for a given property. Considers {@link LazyLoadingProxy},
 * registered {@link Object} to {@link DocumentPointer} {@link org.springframework.core.convert.converter.Converter},
 * simple {@literal _id} lookups and cases where the {@link DocumentPointer} needs to be computed via a lookup query.
 *
 * @author Christoph Strobl
 * @since 3.3
 */
class DocumentPointerFactory {

	private final ConversionService conversionService;
	private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
	private final Map<String, LinkageDocument> cache;

	/**
	 * A {@link Pattern} matching quoted and unquoted variants (with/out whitespaces) of
	 * <code>{'_id' : ?#{#target} }</code>.
	 */
	private static final Pattern DEFAULT_LOOKUP_PATTERN = Pattern.compile("\\{\\s?" + // document start (whitespace opt)
			"['\"]?_id['\"]?" + // followed by an optionally quoted _id. Like: _id, '_id' or "_id"
			"?\\s?:\\s?" + // then a colon optionally wrapped inside whitespaces
			"['\"]?\\?#\\{#target\\}['\"]?" + // leading to the potentially quoted ?#{#target} expression
			"\\s*}"); // some optional whitespaces and document close

	DocumentPointerFactory(ConversionService conversionService,
			MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext) {

		this.conversionService = conversionService;
		this.mappingContext = mappingContext;
		this.cache = new WeakHashMap<>();
	}

	DocumentPointer<?> computePointer(
			MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext,
			MongoPersistentProperty property, Object value, Class<?> typeHint) {

		if (value instanceof LazyLoadingProxy proxy) {
			return proxy::getSource;
		}

		if (conversionService.canConvert(typeHint, DocumentPointer.class)) {
			return conversionService.convert(value, DocumentPointer.class);
		}

		MongoPersistentEntity<?> persistentEntity = mappingContext
				.getRequiredPersistentEntity(property.getAssociationTargetType());

		if (usesDefaultLookup(property)) {

			MongoPersistentProperty idProperty = persistentEntity.getIdProperty();
			Object idValue = persistentEntity.getIdentifierAccessor(value).getIdentifier();

			if (idProperty.hasExplicitWriteTarget()
					&& conversionService.canConvert(idValue.getClass(), idProperty.getFieldType())) {
				return () -> conversionService.convert(idValue, idProperty.getFieldType());
			}

			if (idValue instanceof String stringValue && ObjectId.isValid((String) idValue)) {
				return () -> new ObjectId(stringValue);
			}

			return () -> idValue;
		}

		MongoPersistentEntity<?> valueEntity = mappingContext.getPersistentEntity(value.getClass());
		PersistentPropertyAccessor<Object> propertyAccessor;
		if (valueEntity == null) {
			propertyAccessor = BeanWrapperPropertyAccessorFactory.INSTANCE.getPropertyAccessor(property.getOwner(), value);
		} else {
			propertyAccessor = valueEntity.getPropertyPathAccessor(value);
		}

		return cache.computeIfAbsent(property.getDocumentReference().lookup(), LinkageDocument::from)
				.getDocumentPointer(mappingContext, persistentEntity, propertyAccessor);
	}

	private boolean usesDefaultLookup(MongoPersistentProperty property) {

		if (property.isDocumentReference()) {
			return DEFAULT_LOOKUP_PATTERN.matcher(property.getDocumentReference().lookup()).matches();
		}

		Reference atReference = property.findAnnotation(Reference.class);
		if (atReference != null) {
			return true;
		}

		throw new IllegalStateException(String.format("%s does not seem to be define Reference", property));
	}

	/**
	 * Value object that computes a document pointer from a given lookup query by identifying SpEL expressions and
	 * inverting it.
	 *
	 * <pre class="code">
	 * // source
	 * { 'firstname' : ?#{fn}, 'lastname' : '?#{ln} }
	 *
	 * // target
	 * { 'fn' : ..., 'ln' : ... }
	 * </pre>
	 *
	 * The actual pointer is the computed via
	 * {@link #getDocumentPointer(MappingContext, MongoPersistentEntity, PersistentPropertyAccessor)} applying values from
	 * the provided {@link PersistentPropertyAccessor} to the target document by looking at the keys of the expressions
	 * from the source.
	 */
	static class LinkageDocument {

		static final Pattern EXPRESSION_PATTERN = Pattern.compile("\\?#\\{#?(?<fieldName>[\\w\\d\\.\\-)]*)\\}");
		static final Pattern PLACEHOLDER_PATTERN = Pattern.compile("###_(?<index>\\d*)_###");

		private final String lookup;
		private final org.bson.Document documentPointer;
		private final Map<String, String> placeholderMap;
		private final boolean isSimpleTargetPointer;

		static LinkageDocument from(String lookup) {
			return new LinkageDocument(lookup);
		}

		private LinkageDocument(String lookup) {

			this.lookup = lookup;
			this.placeholderMap = new LinkedHashMap<>();

			int index = 0;
			Matcher matcher = EXPRESSION_PATTERN.matcher(lookup);
			String targetLookup = lookup;

			while (matcher.find()) {

				String expression = matcher.group();
				String fieldName = matcher.group("fieldName").replace("target.", "");

				String placeholder = placeholder(index);
				placeholderMap.put(placeholder, fieldName);
				targetLookup = targetLookup.replace(expression, "'" + placeholder + "'");
				index++;
			}

			this.documentPointer = org.bson.Document.parse(targetLookup);
			this.isSimpleTargetPointer = placeholderMap.size() == 1 && placeholderMap.containsValue("target")
					&& lookup.contains("#target");
		}

		private String placeholder(int index) {
			return "###_" + index + "_###";
		}

		private boolean isPlaceholder(String key) {
			return PLACEHOLDER_PATTERN.matcher(key).matches();
		}

		DocumentPointer<Object> getDocumentPointer(
				MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext,
				MongoPersistentEntity<?> persistentEntity, PersistentPropertyAccessor<?> propertyAccessor) {
			return () -> updatePlaceholders(documentPointer, new Document(), mappingContext, persistentEntity,
					propertyAccessor);
		}

		Object updatePlaceholders(org.bson.Document source, org.bson.Document target,
				MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext,
				MongoPersistentEntity<?> persistentEntity, PersistentPropertyAccessor<?> propertyAccessor) {

			for (Entry<String, Object> entry : source.entrySet()) {

				if (entry.getKey().startsWith("$")) {
					throw new InvalidDataAccessApiUsageException(String.format(
							"Cannot derive document pointer from lookup '%s' using query operator (%s); Please consider registering a custom converter",
							lookup, entry.getKey()));
				}

				if (entry.getValue() instanceof Document document) {

					MongoPersistentProperty persistentProperty = persistentEntity.getPersistentProperty(entry.getKey());
					if (persistentProperty != null && persistentProperty.isEntity()) {

						MongoPersistentEntity<?> nestedEntity = mappingContext.getPersistentEntity(persistentProperty.getType());
						target.put(entry.getKey(), updatePlaceholders(document, new Document(), mappingContext,
								nestedEntity, nestedEntity.getPropertyAccessor(propertyAccessor.getProperty(persistentProperty))));
					} else {
						target.put(entry.getKey(), updatePlaceholders((Document) entry.getValue(), new Document(), mappingContext,
								persistentEntity, propertyAccessor));
					}
					continue;
				}

				if (placeholderMap.containsKey(entry.getValue())) {

					String attribute = placeholderMap.get(entry.getValue());
					if (attribute.contains(".")) {
						attribute = attribute.substring(attribute.lastIndexOf('.') + 1);
					}

					String fieldName = entry.getKey().equals(FieldName.ID.name()) ? "id" : entry.getKey();
					if (!fieldName.contains(".")) {

						Object targetValue = propertyAccessor.getProperty(persistentEntity.getPersistentProperty(fieldName));
						target.put(attribute, targetValue);
						continue;
					}

					PersistentPropertyPathAccessor<?> propertyPathAccessor = persistentEntity
							.getPropertyPathAccessor(propertyAccessor.getBean());
					PersistentPropertyPath<?> path = mappingContext
							.getPersistentPropertyPath(PropertyPath.from(fieldName, persistentEntity.getTypeInformation()));
					Object targetValue = propertyPathAccessor.getProperty(path);
					target.put(attribute, targetValue);
					continue;
				}

				target.put(entry.getKey(), entry.getValue());
			}

			if (target.size() == 1 && isSimpleTargetPointer) {
				return target.values().iterator().next();
			}

			return target;
		}
	}
}
