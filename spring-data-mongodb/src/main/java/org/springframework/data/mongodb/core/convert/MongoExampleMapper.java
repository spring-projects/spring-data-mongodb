/*
 * Copyright 2015-2016 the original author or authors.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.regex.Pattern;

import org.springframework.data.domain.Example;
import org.springframework.data.domain.ExampleSpec;
import org.springframework.data.domain.ExampleSpec.PropertyValueTransformer;
import org.springframework.data.domain.ExampleSpecAccessor;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.query.MongoRegexCreator;
import org.springframework.data.mongodb.core.query.SerializationUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import static org.springframework.data.domain.ExampleSpec.*;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.8
 */
public class MongoExampleMapper {

	private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
	private final MongoConverter converter;

	public MongoExampleMapper(MongoConverter converter) {

		this.converter = converter;
		this.mappingContext = converter.getMappingContext();
	}

	/**
	 * Returns the given {@link Example} as {@link DBObject} holding matching values extracted from
	 * {@link Example#getProbe()}.
	 *
	 * @param example
	 * @return
	 * @since 1.8
	 */
	public DBObject getMappedExample(Example<?> example) {
		return getMappedExample(example, mappingContext.getPersistentEntity(example.getProbeType()));
	}

	/**
	 * Returns the given {@link Example} as {@link DBObject} holding matching values extracted from
	 * {@link Example#getProbe()}.
	 *
	 * @param example
	 * @param entity
	 * @return
	 * @since 1.8
	 */
	public DBObject getMappedExample(Example<?> example, MongoPersistentEntity<?> entity) {

		DBObject reference = (DBObject) converter.convertToMongoType(example.getProbe());

		if (entity.hasIdProperty() && entity.getIdentifierAccessor(example.getProbe()).getIdentifier() == null) {
			reference.removeField(entity.getIdProperty().getFieldName());
		}

		ExampleSpecAccessor exampleSpecAccessor = new ExampleSpecAccessor(example.getExampleSpec());

		applyPropertySpecs("", reference, example.getProbeType(), exampleSpecAccessor);

		return ObjectUtils.nullSafeEquals( NullHandler.INCLUDE, exampleSpecAccessor.getNullHandler()) ? reference
				: new BasicDBObject(SerializationUtils.flatMap(reference));
	}

	private String getMappedPropertyPath(String path, Class<?> probeType, ExampleSpecAccessor exampleSpecAccessor) {

		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(probeType);

		Iterator<String> parts = Arrays.asList(path.split("\\.")).iterator();

		final Stack<MongoPersistentProperty> stack = new Stack<MongoPersistentProperty>();

		List<String> resultParts = new ArrayList<String>();

		while (parts.hasNext()) {

			final String part = parts.next();
			MongoPersistentProperty prop = entity.getPersistentProperty(part);

			if (prop == null) {

				entity.doWithProperties(new PropertyHandler<MongoPersistentProperty>() {

					@Override
					public void doWithPersistentProperty(MongoPersistentProperty property) {

						if (property.getFieldName().equals(part)) {
							stack.push(property);
						}
					}
				});

				if (stack.isEmpty()) {
					return "";
				}
				prop = stack.pop();
			}

			resultParts.add(prop.getName());

			if (prop.isEntity() && mappingContext.hasPersistentEntityFor(prop.getActualType())) {
				entity = mappingContext.getPersistentEntity(prop.getActualType());
			} else {
				break;
			}
		}

		return StringUtils.collectionToDelimitedString(resultParts, ".");

	}

	private void applyPropertySpecs(String path, DBObject source, Class<?> probeType,
			ExampleSpecAccessor exampleSpecAccessor) {

		if (!(source instanceof BasicDBObject)) {
			return;
		}

		Iterator<Map.Entry<String, Object>> iter = ((BasicDBObject) source).entrySet().iterator();

		while (iter.hasNext()) {

			Map.Entry<String, Object> entry = iter.next();

			if (entry.getKey().equals("_id") && entry.getValue() == null) {
				iter.remove();
				continue;
			}

			String propertyPath = StringUtils.hasText(path) ? path + "." + entry.getKey() : entry.getKey();

			String mappedPropertyPath = getMappedPropertyPath(propertyPath, probeType, exampleSpecAccessor);
			if (exampleSpecAccessor.isIgnoredPath(propertyPath) || exampleSpecAccessor.isIgnoredPath(mappedPropertyPath)) {
				iter.remove();
				continue;
			}

			StringMatcher stringMatcher = exampleSpecAccessor.getDefaultStringMatcher();
			Object value = entry.getValue();
			boolean ignoreCase = exampleSpecAccessor.isIgnoreCaseEnabled();

			if (exampleSpecAccessor.hasPropertySpecifiers()) {

				mappedPropertyPath = exampleSpecAccessor.hasPropertySpecifier(propertyPath) ? propertyPath
						: getMappedPropertyPath(propertyPath, probeType, exampleSpecAccessor);

				stringMatcher = exampleSpecAccessor.getStringMatcherForPath(mappedPropertyPath);
				ignoreCase = exampleSpecAccessor.isIgnoreCaseForPath(mappedPropertyPath);
			}

			// TODO: should a PropertySpecifier outrule the later on string matching?
			if (exampleSpecAccessor.hasPropertySpecifier(mappedPropertyPath)) {

				PropertyValueTransformer valueTransformer = exampleSpecAccessor.getValueTransformerForPath(mappedPropertyPath);
				value = valueTransformer.convert(value);
				if (value == null) {
					iter.remove();
					continue;
				}

				entry.setValue(value);
			}

			if (entry.getValue() instanceof String) {
				applyStringMatcher(entry, stringMatcher, ignoreCase);
			} else if (entry.getValue() instanceof BasicDBObject) {
				applyPropertySpecs(propertyPath, (BasicDBObject) entry.getValue(), probeType, exampleSpecAccessor);
			}
		}
	}

	private void applyStringMatcher(Map.Entry<String, Object> entry, StringMatcher stringMatcher,
			boolean ignoreCase) {

		BasicDBObject dbo = new BasicDBObject();

		if (ObjectUtils.nullSafeEquals(StringMatcher.DEFAULT, stringMatcher)) {

			if (ignoreCase) {
				dbo.put("$regex", Pattern.quote((String) entry.getValue()));
				entry.setValue(dbo);
			}
		} else {

			String expression = MongoRegexCreator.INSTANCE.toRegularExpression((String) entry.getValue(),
					stringMatcher.getPartType());
			dbo.put("$regex", expression);
			entry.setValue(dbo);
		}

		if (ignoreCase) {
			dbo.put("$options", "i");
		}
	}
}
