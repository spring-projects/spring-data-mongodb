/*
 * Copyright 2011-2025 the original author or authors.
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
package org.springframework.data.mongodb.core.mapping;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jspecify.annotations.Nullable;
import org.springframework.data.annotation.Id;
import org.springframework.data.expression.ValueEvaluationContext;
import org.springframework.data.expression.ValueExpression;
import org.springframework.data.expression.ValueExpressionParser;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.AssociationHandler;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.model.BasicPersistentEntity;
import org.springframework.data.mongodb.MongoCollectionUtils;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.util.encryption.EncryptionUtils;
import org.springframework.data.spel.ExpressionDependencies;
import org.springframework.data.util.Lazy;
import org.springframework.data.util.TypeInformation;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;
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
		implements MongoPersistentEntity<T> {

	private static final String AMBIGUOUS_FIELD_MAPPING = "Ambiguous field mapping detected; Both %s and %s map to the same field name %s; Disambiguate using @Field annotation";
	private static final ValueExpressionParser PARSER = ValueExpressionParser.create(SpelExpressionParser::new);

	private final String collection;
	private final String language;

	private final @Nullable ValueExpression expression;

	private final @Nullable String collation;
	private final @Nullable ValueExpression collationExpression;

	private final ShardKey shardKey;

	/**
	 * Creates a new {@link BasicMongoPersistentEntity} with the given {@link TypeInformation}. Will default the
	 * collection name to the entities simple type name.
	 *
	 * @param typeInformation must not be {@literal null}.
	 */
	public BasicMongoPersistentEntity(TypeInformation<T> typeInformation) {

		super(typeInformation, MongoPersistentPropertyComparator.INSTANCE);

		Class<?> rawType = typeInformation.getType();
		String fallback = MongoCollectionUtils.getPreferredCollectionName(rawType);

		if (this.isAnnotationPresent(Document.class)) {
			Document document = this.getRequiredAnnotation(Document.class);

			this.collection = StringUtils.hasText(document.collection()) ? document.collection() : fallback;
			this.language = StringUtils.hasText(document.language()) ? document.language() : "";
			this.expression = detectExpression(document.collection());
			this.collation = document.collation();
			this.collationExpression = detectExpression(document.collation());
		} else {

			this.collection = fallback;
			this.language = "";
			this.expression = null;
			this.collation = null;
			this.collationExpression = null;
		}

		this.shardKey = detectShardKey();
	}

	private ShardKey detectShardKey() {

		if (!isAnnotationPresent(Sharded.class)) {
			return ShardKey.none();
		}

		Sharded sharded = getRequiredAnnotation(Sharded.class);

		String[] keyProperties = sharded.shardKey();
		if (ObjectUtils.isEmpty(keyProperties)) {
			keyProperties = new String[] { FieldName.ID.name() };
		}

		ShardKey shardKey = ShardingStrategy.HASH.equals(sharded.shardingStrategy()) ? ShardKey.hash(keyProperties)
				: ShardKey.range(keyProperties);

		return sharded.immutableKey() ? ShardKey.immutable(shardKey) : shardKey;
	}

	@Override
	public String getCollection() {

		return expression == null //
				? collection //
				: ObjectUtils.nullSafeToString(expression.evaluate(getValueEvaluationContext(null)));
	}

	@Override
	public String getLanguage() {
		return this.language;
	}

	@Override
	public @Nullable MongoPersistentProperty getTextScoreProperty() {
		return getPersistentProperty(TextScore.class);
	}

	@Override
	public boolean hasTextScoreProperty() {
		return getTextScoreProperty() != null;
	}

	@Override
	public @Nullable Collation getCollation() {

		Object collationValue = collationExpression != null
				? collationExpression.evaluate(getValueEvaluationContext(null))
				: this.collation;

		if (collationValue == null) {
			return null;
		}

		if (collationValue instanceof org.bson.Document document) {
			return org.springframework.data.mongodb.core.query.Collation.from(document);
		}

		if (collationValue instanceof org.springframework.data.mongodb.core.query.Collation collation) {
			return collation;
		}

		return StringUtils.hasText(collationValue.toString())
				? org.springframework.data.mongodb.core.query.Collation.parse(collationValue.toString())
				: null;
	}

	@Override
	public ShardKey getShardKey() {
		return shardKey;
	}

	@Override
	public void verify() {

		super.verify();

		verifyFieldUniqueness();
		verifyFieldTypes();
	}

	@Override
	public EvaluationContext getEvaluationContext(Object rootObject) {
		return super.getEvaluationContext(rootObject);
	}

	@Override
	public EvaluationContext getEvaluationContext(@Nullable Object rootObject, ExpressionDependencies dependencies) {
		return super.getEvaluationContext(rootObject, dependencies);
	}

	@Override
	public ValueEvaluationContext getValueEvaluationContext(@Nullable Object rootObject) {
		return super.getValueEvaluationContext(rootObject);
	}

	@Override
	public ValueEvaluationContext getValueEvaluationContext(@Nullable Object rootObject, ExpressionDependencies dependencies) {
		return super.getValueEvaluationContext(rootObject, dependencies);
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
	enum MongoPersistentPropertyComparator implements Comparator<MongoPersistentProperty> {

		INSTANCE;

		public int compare(@Nullable MongoPersistentProperty o1, @Nullable MongoPersistentProperty o2) {

			if (o1 != null && o1.getFieldOrder() == Integer.MAX_VALUE) {
				return 1;
			}

			if (o2 != null && o2.getFieldOrder() == Integer.MAX_VALUE) {
				return -1;
			}

			if (o1 == null && o2 == null) {
				return -1;
			}

			if(o1 != null && o2 != null) {
				return o1.getFieldOrder() - o2.getFieldOrder();
			}

			return o1.getFieldOrder();
		}
	}

	/**
	 * As a general note: An implicit id property has a name that matches "id" or "_id". An explicit id property is one
	 * that is annotated with @see {@link Id}. The property id is updated according to the following rules: 1) An id
	 * property which is defined explicitly takes precedence over an implicitly defined id property. 2) In case of any
	 * ambiguity a @see {@link MappingException} is thrown.
	 *
	 * @param property - the new id property candidate
	 * @return can be {@literal null}.
	 */
	@Override
	protected @Nullable MongoPersistentProperty returnPropertyIfBetterIdPropertyCandidateOrNull(MongoPersistentProperty property) {

		Assert.notNull(property, "MongoPersistentProperty must not be null");

		if (!property.isIdProperty()) {
			return null;
		}

		MongoPersistentProperty currentIdProperty = getIdProperty();

		boolean currentIdPropertyIsSet = currentIdProperty != null;
		@SuppressWarnings("NullAway")
		boolean currentIdPropertyIsExplicit = currentIdPropertyIsSet && currentIdProperty.isExplicitIdProperty();
		boolean newIdPropertyIsExplicit = property.isExplicitIdProperty();

		if (!currentIdPropertyIsSet) {
			return property;

		}

		@SuppressWarnings("null")
		Field currentIdPropertyField = currentIdProperty.getField();

		if (newIdPropertyIsExplicit && currentIdPropertyIsExplicit) {
			throw new MappingException(
					String.format("Attempt to add explicit id property %s but already have an property %s registered "
							+ "as explicit id; Check your mapping configuration", property.getField(), currentIdPropertyField));

		} else if (newIdPropertyIsExplicit && !currentIdPropertyIsExplicit) {
			// explicit id property takes precedence over implicit id property
			return property;

		} else if (!newIdPropertyIsExplicit && currentIdPropertyIsExplicit) {
			// no id property override - current property is explicitly defined

		} else {
			throw new MappingException(
					String.format("Attempt to add id property %s but already have an property %s registered "
							+ "as id; Check your mapping configuration", property.getField(), currentIdPropertyField));
		}

		return null;
	}

	/**
	 * Returns a Value {@link Expression} if the given {@link String} is actually an expression that does not evaluate to
	 * a literal expression (indicating that no subsequent evaluation is necessary).
	 *
	 * @param potentialExpression can be {@literal null}
	 * @return can be {@literal null}.
	 */
	private static @Nullable ValueExpression detectExpression(@Nullable String potentialExpression) {

		if (!StringUtils.hasText(potentialExpression)) {
			return null;
		}

		ValueExpression expression = PARSER.parse(potentialExpression);
		return expression.isLiteral() ? null : expression;
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
				throw new MappingException(String.format(AMBIGUOUS_FIELD_MAPPING, property, existingProperty, fieldName));
			}

			properties.put(fieldName, property);
		}
	}

	@Override
	public Collection<Object> getEncryptionKeyIds() {

		Encrypted encrypted = findAnnotation(Encrypted.class);
		if (encrypted == null) {
			return null;
		}

		if (ObjectUtils.isEmpty(encrypted.keyId())) {
			return Collections.emptySet();
		}

		Lazy<EvaluationContext> evaluationContext = Lazy.of(() -> {

			EvaluationContext ctx = getEvaluationContext(null);
			ctx.setVariable("target", getType().getSimpleName());
			return ctx;
		});

		List<Object> target = new ArrayList<>();
		for (String keyId : encrypted.keyId()) {
			target.add(EncryptionUtils.resolveKeyId(keyId, evaluationContext));
		}
		return target;
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.6
	 */
	private static class PropertyTypeAssertionHandler implements PropertyHandler<MongoPersistentProperty> {

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
					throw new MappingException(
							String.format("Invalid lazy DBRef property for %s; Found %s which must not be an array nor a final class",
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
					String.format("Mismatching types for %s; Found %s expected one of %s", persistentProperty.getField(),
							persistentProperty.getActualType(), StringUtils.arrayToCommaDelimitedString(validMatches)));
		}
	}
}
