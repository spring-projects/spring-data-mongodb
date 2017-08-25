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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;
import java.util.regex.Pattern;

import org.springframework.data.domain.Example;
import org.springframework.data.domain.ExampleMatcher;
import org.springframework.data.domain.ExampleMatcher.NullHandler;
import org.springframework.data.domain.ExampleMatcher.PropertyValueTransformer;
import org.springframework.data.domain.ExampleMatcher.StringMatcher;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.query.MongoRegexCreator;
import org.springframework.data.mongodb.core.query.SerializationUtils;
import org.springframework.data.repository.core.support.ExampleMatcherAccessor;
import org.springframework.data.repository.query.parser.Part.Type;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Mapper from {@link Example} to a query {@link DBObject}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.8
 * @see Example
 * @see org.springframework.data.domain.ExampleMatcher
 */
public class MongoExampleMapper {

	private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
	private final MongoConverter converter;
	private final Map<StringMatcher, Type> stringMatcherPartMapping = new HashMap<StringMatcher, Type>();

	/**
	 * Create a new {@link MongoTypeMapper} given {@link MongoConverter}.
	 *
	 * @param converter must not be {@literal null}.
	 */
	public MongoExampleMapper(MongoConverter converter) {

		this.converter = converter;
		this.mappingContext = converter.getMappingContext();

		stringMatcherPartMapping.put(StringMatcher.EXACT, Type.SIMPLE_PROPERTY);
		stringMatcherPartMapping.put(StringMatcher.CONTAINING, Type.CONTAINING);
		stringMatcherPartMapping.put(StringMatcher.STARTING, Type.STARTING_WITH);
		stringMatcherPartMapping.put(StringMatcher.ENDING, Type.ENDING_WITH);
		stringMatcherPartMapping.put(StringMatcher.REGEX, Type.REGEX);
	}

	/**
	 * Returns the given {@link Example} as {@link DBObject} holding matching values extracted from
	 * {@link Example#getProbe()}.
	 *
	 * @param example must not be {@literal null}.
	 * @return
	 */
	public DBObject getMappedExample(Example<?> example) {

		Assert.notNull(example, "Example must not be null!");

		return getMappedExample(example, mappingContext.getPersistentEntity(example.getProbeType()));
	}

	/**
	 * Returns the given {@link Example} as {@link DBObject} holding matching values extracted from
	 * {@link Example#getProbe()}.
	 *
	 * @param example must not be {@literal null}.
	 * @param entity must not be {@literal null}.
	 * @return
	 */
	public DBObject getMappedExample(Example<?> example, MongoPersistentEntity<?> entity) {

		Assert.notNull(example, "Example must not be null!");
		Assert.notNull(entity, "MongoPersistentEntity must not be null!");

		DBObject reference = (DBObject) converter.convertToMongoType(example.getProbe());

		if (entity.hasIdProperty() && ClassUtils.isAssignable(entity.getType(), example.getProbeType())) {
			if (entity.getIdentifierAccessor(example.getProbe()).getIdentifier() == null) {
				reference.removeField(entity.getIdProperty().getFieldName());
			}
		}

		ExampleMatcherAccessor matcherAccessor = new ExampleMatcherAccessor(example.getMatcher());

		applyPropertySpecs("", reference, example.getProbeType(), matcherAccessor);

		DBObject flattened = ObjectUtils.nullSafeEquals(NullHandler.INCLUDE, matcherAccessor.getNullHandler()) ? reference
				: new BasicDBObject(SerializationUtils.flattenMap(reference));
		DBObject result = example.getMatcher().isAllMatching() ? flattened : orConcatenate(flattened);

		return updateTypeRestrictions(result, example);
	}

	private void applyPropertySpecs(String path, DBObject source, Class<?> probeType,
			ExampleMatcherAccessor exampleSpecAccessor) {

		if (!(source instanceof BasicDBObject)) {
			return;
		}

		Iterator<Map.Entry<String, Object>> iter = ((BasicDBObject) source).entrySet().iterator();

		while (iter.hasNext()) {

			Map.Entry<String, Object> entry = iter.next();
			String propertyPath = StringUtils.hasText(path) ? path + "." + entry.getKey() : entry.getKey();
			String mappedPropertyPath = getMappedPropertyPath(propertyPath, probeType);

			if (isEmptyIdProperty(entry)) {
				iter.remove();
				continue;
			}

			if (exampleSpecAccessor.isIgnoredPath(propertyPath) || exampleSpecAccessor.isIgnoredPath(mappedPropertyPath)) {
				iter.remove();
				continue;
			}

			StringMatcher stringMatcher = exampleSpecAccessor.getDefaultStringMatcher();
			Object value = entry.getValue();
			boolean ignoreCase = exampleSpecAccessor.isIgnoreCaseEnabled();

			if (exampleSpecAccessor.hasPropertySpecifiers()) {

				mappedPropertyPath = exampleSpecAccessor.hasPropertySpecifier(propertyPath) ? propertyPath
						: getMappedPropertyPath(propertyPath, probeType);

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

	private String getMappedPropertyPath(String path, Class<?> probeType) {

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

	private DBObject updateTypeRestrictions(DBObject query, Example example) {

		DBObject result = new BasicDBObject();

		if (isTypeRestricting(example.getMatcher())) {

			result.putAll(query);
			this.converter.getTypeMapper().writeTypeRestrictions(result, getTypesToMatch(example));
			return result;
		}

		for (String key : query.keySet()) {
			if (!this.converter.getTypeMapper().isTypeKey(key)) {
				result.put(key, query.get(key));
			}
		}

		return result;
	}

	private boolean isTypeRestricting(ExampleMatcher matcher) {

		if (matcher.getIgnoredPaths().isEmpty()) {
			return true;
		}

		for (String path : matcher.getIgnoredPaths()) {
			if (this.converter.getTypeMapper().isTypeKey(path)) {
				return false;
			}
		}

		return true;
	}

	private Set<Class<?>> getTypesToMatch(Example<?> example) {

		Set<Class<?>> types = new HashSet<Class<?>>();

		for (TypeInformation<?> reference : mappingContext.getManagedTypes()) {
			if (example.getProbeType().isAssignableFrom(reference.getType())) {
				types.add(reference.getType());
			}
		}

		return types;
	}

	private static boolean isEmptyIdProperty(Entry<String, Object> entry) {
		return entry.getKey().equals("_id") && entry.getValue() == null;
	}

	private void applyStringMatcher(Map.Entry<String, Object> entry, StringMatcher stringMatcher, boolean ignoreCase) {

		BasicDBObject dbo = new BasicDBObject();

		if (ObjectUtils.nullSafeEquals(StringMatcher.DEFAULT, stringMatcher)) {

			if (ignoreCase) {
				dbo.put("$regex", Pattern.quote((String) entry.getValue()));
				entry.setValue(dbo);
			}
		} else {

			Type type = stringMatcherPartMapping.get(stringMatcher);
			String expression = MongoRegexCreator.INSTANCE.toRegularExpression((String) entry.getValue(), type);
			dbo.put("$regex", expression);
			entry.setValue(dbo);
		}

		if (ignoreCase) {
			dbo.put("$options", "i");
		}
	}

	private static DBObject orConcatenate(DBObject source) {

		List<DBObject> or = new ArrayList<DBObject>(source.keySet().size());

		for (String key : source.keySet()) {
			or.add(new BasicDBObject(key, source.get(key)));
		}

		return new BasicDBObject("$or", or);
	}
}
