/*
 * Copyright 2021 the original author or authors.
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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.bson.Document;

import org.springframework.core.convert.ConversionService;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.BeanWrapperPropertyAccessorFactory;
import org.springframework.data.mongodb.core.mapping.DocumentPointer;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;

/**
 * @author Christoph Strobl
 * @since 3.3
 */
class DocumentPointerFactory {

	private final ConversionService conversionService;
	private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
	private final Map<String, LinkageDocument> linkageMap;

	public DocumentPointerFactory(ConversionService conversionService,
			MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext) {

		this.conversionService = conversionService;
		this.mappingContext = mappingContext;
		this.linkageMap = new HashMap<>();
	}

	public DocumentPointer<?> computePointer(MongoPersistentProperty property, Object value, Class<?> typeHint) {

		if (value instanceof LazyLoadingProxy) {
			return () -> ((LazyLoadingProxy) value).getSource();
		}

		if (conversionService.canConvert(typeHint, DocumentPointer.class)) {
			return conversionService.convert(value, DocumentPointer.class);
		} else {

			MongoPersistentEntity<?> persistentEntity = mappingContext
					.getRequiredPersistentEntity(property.getAssociationTargetType());

			// TODO: Extract method
			if (!property.getDocumentReference().lookup().toLowerCase(Locale.ROOT).replaceAll("\\s", "").replaceAll("'", "")
					.equals("{_id:?#{#target}}")) {

				MongoPersistentEntity<?> valueEntity = mappingContext.getPersistentEntity(value.getClass());
				PersistentPropertyAccessor<Object> propertyAccessor;
				if (valueEntity == null) {
					propertyAccessor = BeanWrapperPropertyAccessorFactory.INSTANCE.getPropertyAccessor(property.getOwner(),
							value);
				} else {
					propertyAccessor = valueEntity.getPropertyAccessor(value);

				}

				return () -> linkageMap.computeIfAbsent(property.getDocumentReference().lookup(), LinkageDocument::new)
						.get(persistentEntity, propertyAccessor);
			}

			// just take the id as a reference
			return () -> persistentEntity.getIdentifierAccessor(value).getIdentifier();
		}
	}

	static class LinkageDocument {

		static final Pattern pattern = Pattern.compile("\\?#\\{#?[\\w\\d]*\\}");

		String lookup;
		org.bson.Document fetchDocument;
		Map<Integer, String> mapMap;

		public LinkageDocument(String lookup) {

			this.lookup = lookup;
			String targetLookup = lookup;


			Matcher matcher = pattern.matcher(lookup);
			int index = 0;
			mapMap = new LinkedHashMap<>();

			// TODO: Make explicit what's happening here
			while (matcher.find()) {

				String expr = matcher.group();
				String sanitized = expr.substring(0, expr.length() - 1).replace("?#{#", "").replace("?#{", "")
						.replace("target.", "").replaceAll("'", "");
				mapMap.put(index, sanitized);
				targetLookup = targetLookup.replace(expr, index + "");
				index++;
			}

			fetchDocument = org.bson.Document.parse(targetLookup);
		}

		org.bson.Document get(MongoPersistentEntity<?> persistentEntity, PersistentPropertyAccessor<?> propertyAccessor) {

			org.bson.Document targetDocument = new Document();

			// TODO: recursive matching over nested Documents or would the parameter binding json parser be a thing?
			// like we have it ordered by index values and could provide the parameter array from it.

			for (Entry<String, Object> entry : fetchDocument.entrySet()) {

				if (entry.getKey().equals("target")) {

					String refKey = mapMap.get(entry.getValue());

					if (persistentEntity.hasIdProperty()) {
						targetDocument.put(refKey, propertyAccessor.getProperty(persistentEntity.getIdProperty()));
					} else {
						targetDocument.put(refKey, propertyAccessor.getBean());
					}
					continue;
				}

				Object target = propertyAccessor.getProperty(persistentEntity.getPersistentProperty(entry.getKey()));
				String refKey = mapMap.get(entry.getValue());
				targetDocument.put(refKey, target);
			}
			return targetDocument;
		}
	}
}
