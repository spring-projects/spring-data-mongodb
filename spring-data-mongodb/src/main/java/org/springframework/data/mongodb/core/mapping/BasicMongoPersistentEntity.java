/*
 * Copyright 2011-2017 the original author or authors.
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
package org.springframework.data.mongodb.core.mapping;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.expression.BeanFactoryAccessor;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.AssociationHandler;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.model.BasicPersistentEntity;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.mongodb.MongoCollectionUtils;
import org.springframework.data.util.TypeInformation;
import org.springframework.expression.Expression;
import org.springframework.expression.ParserContext;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * MongoDB specific {@link MongoPersistentEntity} implementation that adds Mongo specific meta-data such as the
 * collection name and the like.
 * 
 * @author Jon Brisbin
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class BasicMongoPersistentEntity<T> extends BasicPersistentEntity<T, MongoPersistentProperty>
		implements MongoPersistentEntity<T>, ApplicationContextAware {

	private static final String AMBIGUOUS_FIELD_MAPPING = "Ambiguous field mapping detected! Both %s and %s map to the same field name %s! Disambiguate using @Field annotation!";
	private static final SpelExpressionParser PARSER = new SpelExpressionParser();

	private final String collection;
	private final String language;

	private final StandardEvaluationContext context;
	private final Expression expression;

	/**
	 * Creates a new {@link BasicMongoPersistentEntity} with the given {@link TypeInformation}. Will default the
	 * collection name to the entities simple type name.
	 * 
	 * @param typeInformation must not be {@literal null}.
	 */
	public BasicMongoPersistentEntity(TypeInformation<T> typeInformation) {

		super(typeInformation, Optional.of(MongoPersistentPropertyComparator.INSTANCE));

		Class<?> rawType = typeInformation.getType();
		String fallback = MongoCollectionUtils.getPreferredCollectionName(rawType);

		Optional<Document> document = this.findAnnotation(Document.class);

		this.expression = document.map(it -> detectExpression(it)).orElse(null);
		this.context = new StandardEvaluationContext();
		this.collection = document.filter(it -> StringUtils.hasText(it.collection())).map(it -> it.collection())
				.orElse(fallback);
		this.language = document.filter(it -> StringUtils.hasText(it.language())).map(it -> it.language()).orElse("");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.context.ApplicationContextAware#setApplicationContext(org.springframework.context.ApplicationContext)
	 */
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {

		context.addPropertyAccessor(new BeanFactoryAccessor());
		context.setBeanResolver(new BeanFactoryResolver(applicationContext));
		context.setRootObject(applicationContext);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.MongoPersistentEntity#getCollection()
	 */
	public String getCollection() {
		return expression == null ? collection : expression.getValue(context, String.class);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.MongoPersistentEntity#getLanguage()
	 */
	@Override
	public String getLanguage() {
		return this.language;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.MongoPersistentEntity#getTextScoreProperty()
	 */
	@Override
	public MongoPersistentProperty getTextScoreProperty() {
		return getPersistentProperty(TextScore.class).orElse(null);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.MongoPersistentEntity#hasTextScoreProperty()
	 */
	@Override
	public boolean hasTextScoreProperty() {
		return getTextScoreProperty() != null;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.model.BasicPersistentEntity#verify()
	 */
	@Override
	public void verify() {

		verifyFieldUniqueness();
		verifyFieldTypes();
	}

	private void verifyFieldUniqueness() {

		AssertFieldNameUniquenessHandler handler = new AssertFieldNameUniquenessHandler();

		doWithProperties(handler);
		doWithAssociations(handler);
	}

	private void verifyFieldTypes() {
		doWithProperties(new PropertyTypeAssertionHandler());
	}

	/**
	 * {@link Comparator} implementation inspecting the {@link MongoPersistentProperty}'s order.
	 * 
	 * @author Oliver Gierke
	 */
	static enum MongoPersistentPropertyComparator implements Comparator<MongoPersistentProperty> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
		 */
		public int compare(MongoPersistentProperty o1, MongoPersistentProperty o2) {

			if (o1.getFieldOrder() == Integer.MAX_VALUE) {
				return 1;
			}

			if (o2.getFieldOrder() == Integer.MAX_VALUE) {
				return -1;
			}

			return o1.getFieldOrder() - o2.getFieldOrder();
		}
	}

	/**
	 * As a general note: An implicit id property has a name that matches "id" or "_id". An explicit id property is one
	 * that is annotated with @see {@link Id}. The property id is updated according to the following rules: 1) An id
	 * property which is defined explicitly takes precedence over an implicitly defined id property. 2) In case of any
	 * ambiguity a @see {@link MappingException} is thrown.
	 * 
	 * @param property - the new id property candidate
	 * @return
	 */
	@Override
	protected MongoPersistentProperty returnPropertyIfBetterIdPropertyCandidateOrNull(MongoPersistentProperty property) {

		Assert.notNull(property, "MongoPersistentProperty must not be null!");

		if (!property.isIdProperty()) {
			return null;
		}

		Optional<MongoPersistentProperty> currentIdProperty = getIdProperty();

		return currentIdProperty.map(it -> {

			boolean currentIdPropertyIsExplicit = it.isExplicitIdProperty();
			boolean newIdPropertyIsExplicit = property.isExplicitIdProperty();
			Optional<Field> currentIdPropertyField = it.getField();

			if (newIdPropertyIsExplicit && currentIdPropertyIsExplicit) {
				throw new MappingException(
						String.format(
								"Attempt to add explicit id property %s but already have an property %s registered "
										+ "as explicit id. Check your mapping configuration!",
								property.getField(), currentIdPropertyField));

			} else if (newIdPropertyIsExplicit && !currentIdPropertyIsExplicit) {
				// explicit id property takes precedence over implicit id property
				return property;

			} else if (!newIdPropertyIsExplicit && currentIdPropertyIsExplicit) {
				// no id property override - current property is explicitly defined

			} else {
				throw new MappingException(
						String.format("Attempt to add id property %s but already have an property %s registered "
								+ "as id. Check your mapping configuration!", property.getField(), currentIdPropertyField));
			}

			return null;

		}).orElse(property);

	}

	/**
	 * Returns a SpEL {@link Expression} frór the collection String expressed in the given {@link Document} annotation if
	 * present or {@literal null} otherwise. Will also return {@literal null} it the collection {@link String} evaluates
	 * to a {@link LiteralExpression} (indicating that no subsequent evaluation is necessary).
	 * 
	 * @param document can be {@literal null}
	 * @return
	 */
	private static Expression detectExpression(Document document) {

		if (document == null) {
			return null;
		}

		String collection = document.collection();

		if (!StringUtils.hasText(collection)) {
			return null;
		}

		Expression expression = PARSER.parseExpression(document.collection(), ParserContext.TEMPLATE_EXPRESSION);

		return expression instanceof LiteralExpression ? null : expression;
	}

	/**
	 * Handler to collect {@link MongoPersistentProperty} instances and check that each of them is mapped to a distinct
	 * field name.
	 * 
	 * @author Oliver Gierke
	 */
	private static class AssertFieldNameUniquenessHandler
			implements PropertyHandler<MongoPersistentProperty>, AssociationHandler<MongoPersistentProperty> {

		private final Map<String, MongoPersistentProperty> properties = new HashMap<String, MongoPersistentProperty>();

		public void doWithPersistentProperty(MongoPersistentProperty persistentProperty) {
			assertUniqueness(persistentProperty);
		}

		public void doWithAssociation(Association<MongoPersistentProperty> association) {
			assertUniqueness(association.getInverse());
		}

		private void assertUniqueness(MongoPersistentProperty property) {

			String fieldName = property.getFieldName();
			MongoPersistentProperty existingProperty = properties.get(fieldName);

			if (existingProperty != null) {
				throw new MappingException(
						String.format(AMBIGUOUS_FIELD_MAPPING, property.toString(), existingProperty.toString(), fieldName));
			}

			properties.put(fieldName, property);
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.6
	 */
	private static class PropertyTypeAssertionHandler implements PropertyHandler<MongoPersistentProperty> {

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mapping.PropertyHandler#doWithPersistentProperty(org.springframework.data.mapping.PersistentProperty)
		 */
		@Override
		public void doWithPersistentProperty(MongoPersistentProperty persistentProperty) {

			potentiallyAssertTextScoreType(persistentProperty);
			potentiallyAssertLanguageType(persistentProperty);
			potentiallyAssertDBRefTargetType(persistentProperty);
		}

		private static void potentiallyAssertLanguageType(MongoPersistentProperty persistentProperty) {

			if (persistentProperty.isExplicitLanguageProperty()) {
				assertPropertyType(persistentProperty, String.class);
			}
		}

		private static void potentiallyAssertTextScoreType(MongoPersistentProperty persistentProperty) {

			if (persistentProperty.isTextScoreProperty()) {
				assertPropertyType(persistentProperty, Float.class, Double.class);
			}
		}

		private static void potentiallyAssertDBRefTargetType(MongoPersistentProperty persistentProperty) {

			if (persistentProperty.isDbReference() && persistentProperty.getDBRef().lazy()) {
				if (persistentProperty.isArray() || Modifier.isFinal(persistentProperty.getActualType().getModifiers())) {
					throw new MappingException(String.format(
							"Invalid lazy DBRef property for %s. Found %s which must not be an array nor a final class.",
							persistentProperty.getField(), persistentProperty.getActualType()));
				}
			}
		}

		private static void assertPropertyType(MongoPersistentProperty persistentProperty, Class<?>... validMatches) {

			for (Class<?> potentialMatch : validMatches) {
				if (ClassUtils.isAssignable(potentialMatch, persistentProperty.getActualType())) {
					return;
				}
			}

			throw new MappingException(
					String.format("Missmatching types for %s. Found %s expected one of %s.", persistentProperty.getField(),
							persistentProperty.getActualType(), StringUtils.arrayToCommaDelimitedString(validMatches)));
		}
	}
}
