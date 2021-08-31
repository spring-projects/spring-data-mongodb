/*
 * Copyright 2011-2021 the original author or authors.
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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.model.AnnotationBasedPersistentProperty;
import org.springframework.data.mapping.model.FieldNamingStrategy;
import org.springframework.data.mapping.model.Property;
import org.springframework.data.mapping.model.PropertyNameFieldNamingStrategy;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ParserContext;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.lang.Nullable;
import org.springframework.util.StringUtils;

/**
 * MongoDB specific {@link org.springframework.data.mapping.PersistentProperty} implementation.
 *
 * @author Oliver Gierke
 * @author Patryk Wasik
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Divya Srivastava
 */
public class BasicMongoPersistentProperty extends AnnotationBasedPersistentProperty<MongoPersistentProperty>
		implements MongoPersistentProperty {

	private static final Logger LOG = LoggerFactory.getLogger(BasicMongoPersistentProperty.class);

	public static final String ID_FIELD_NAME = "_id";
	private static final String LANGUAGE_FIELD_NAME = "language";
	private static final Set<Class<?>> SUPPORTED_ID_TYPES = new HashSet<Class<?>>();
	private static final Set<String> SUPPORTED_ID_PROPERTY_NAMES = new HashSet<String>();

	static {

		SUPPORTED_ID_TYPES.add(ObjectId.class);
		SUPPORTED_ID_TYPES.add(String.class);
		SUPPORTED_ID_TYPES.add(BigInteger.class);

		SUPPORTED_ID_PROPERTY_NAMES.add("id");
		SUPPORTED_ID_PROPERTY_NAMES.add("_id");
	}

	private final FieldNamingStrategy fieldNamingStrategy;

	/**
	 * Creates a new {@link BasicMongoPersistentProperty}.
	 *
	 * @param property the source property.
	 * @param owner the owing entity.
	 * @param simpleTypeHolder must not be {@literal null}.
	 * @param fieldNamingStrategy can be {@literal null}.
	 */
	public BasicMongoPersistentProperty(Property property, MongoPersistentEntity<?> owner,
			SimpleTypeHolder simpleTypeHolder, @Nullable FieldNamingStrategy fieldNamingStrategy) {

		super(property, owner, simpleTypeHolder);
		this.fieldNamingStrategy = fieldNamingStrategy == null ? PropertyNameFieldNamingStrategy.INSTANCE
				: fieldNamingStrategy;

		if (isIdProperty() && hasExplicitFieldName()) {

			String annotatedName = getAnnotatedFieldName();
			if (!ID_FIELD_NAME.equals(annotatedName)) {
				LOG.warn(
						"Customizing field name for id property '{}.{}' is not allowed! Custom name ('{}') will not be considered!",
						owner.getName(), getName(), annotatedName);
			}
		}
	}

	/**
	 * Also considers fields as id that are of supported id type and name.
	 *
	 * @see #SUPPORTED_ID_PROPERTY_NAMES
	 * @see #SUPPORTED_ID_TYPES
	 */
	@Override
	public boolean isIdProperty() {

		if (super.isIdProperty()) {
			return true;
		}

		// We need to support a wider range of ID types than just the ones that can be converted to an ObjectId
		// but still we need to check if there happens to be an explicit name set
		return SUPPORTED_ID_PROPERTY_NAMES.contains(getName()) && !hasExplicitFieldName();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.MongoPersistentProperty#isExplicitIdProperty()
	 */
	@Override
	public boolean isExplicitIdProperty() {
		return isAnnotationPresent(Id.class);
	}

	/**
	 * Returns the key to be used to store the value of the property inside a Mongo {@link org.bson.Document}.
	 *
	 * @return
	 */
	public String getFieldName() {

		if (isIdProperty()) {

			if (getOwner().getIdProperty() == null) {
				return ID_FIELD_NAME;
			}

			if (getOwner().isIdProperty(this)) {
				return ID_FIELD_NAME;
			}
		}

		if (hasExplicitFieldName()) {
			return getAnnotatedFieldName();
		}

		String fieldName = fieldNamingStrategy.getFieldName(this);

		if (!StringUtils.hasText(fieldName)) {
			throw new MappingException(String.format("Invalid (null or empty) field name returned for property %s by %s!",
					this, fieldNamingStrategy.getClass()));
		}

		return fieldName;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.MongoPersistentProperty#getFieldType()
	 */
	@Override
	public Class<?> getFieldType() {

		Field fieldAnnotation = findAnnotation(Field.class);

		if (!isIdProperty()) {

			if (fieldAnnotation == null || fieldAnnotation.targetType() == FieldType.IMPLICIT) {
				return getType();
			}

			return fieldAnnotation.targetType().getJavaClass();
		}

		if (fieldAnnotation == null) {
			return FieldType.OBJECT_ID.getJavaClass();
		}

		FieldType fieldType = fieldAnnotation.targetType();
		if (fieldType == FieldType.IMPLICIT) {

			if (isEntity()) {
				return org.bson.Document.class;
			}

			return getType();
		}

		return fieldType.getJavaClass();
	}

	/**
	 * @return true if {@link org.springframework.data.mongodb.core.mapping.Field} having non blank
	 *         {@link org.springframework.data.mongodb.core.mapping.Field#value()} present.
	 * @since 1.7
	 */
	protected boolean hasExplicitFieldName() {
		return StringUtils.hasText(getAnnotatedFieldName());
	}

	@Nullable
	private String getAnnotatedFieldName() {

		org.springframework.data.mongodb.core.mapping.Field annotation = findAnnotation(
				org.springframework.data.mongodb.core.mapping.Field.class);

		return annotation != null ? annotation.value() : null;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.MongoPersistentProperty#getFieldOrder()
	 */
	public int getFieldOrder() {

		org.springframework.data.mongodb.core.mapping.Field annotation = findAnnotation(
				org.springframework.data.mongodb.core.mapping.Field.class);

		return annotation != null ? annotation.order() : Integer.MAX_VALUE;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.MongoPersistentProperty#skipNullValues()
	 */
	@Override
	public boolean writeNullValues() {

		org.springframework.data.mongodb.core.mapping.Field annotation = findAnnotation(
				org.springframework.data.mongodb.core.mapping.Field.class);

		return annotation != null && annotation.write() == Field.Write.ALWAYS;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.model.AbstractPersistentProperty#createAssociation()
	 */
	@Override
	protected Association<MongoPersistentProperty> createAssociation() {
		return new Association<MongoPersistentProperty>(this, null);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.MongoPersistentProperty#isDbReference()
	 */
	public boolean isDbReference() {
		return isAnnotationPresent(DBRef.class);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.MongoPersistentProperty#isDocumentReference()
	 */
	@Override
	public boolean isDocumentReference() {
		return isAnnotationPresent(DocumentReference.class);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.MongoPersistentProperty#getDBRef()
	 */
	@Nullable
	public DBRef getDBRef() {
		return findAnnotation(DBRef.class);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.MongoPersistentProperty#getDocumentReference()
	 */
	@Nullable
	@Override
	public DocumentReference getDocumentReference() {
		return findAnnotation(DocumentReference.class);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.MongoPersistentProperty#isLanguageProperty()
	 */
	@Override
	public boolean isLanguageProperty() {
		return getFieldName().equals(LANGUAGE_FIELD_NAME) || isExplicitLanguageProperty();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.MongoPersistentProperty#isExplicitLanguageProperty()
	 */
	@Override
	public boolean isExplicitLanguageProperty() {
		return isAnnotationPresent(Language.class);
	};

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.mapping.MongoPersistentProperty#isTextScoreProperty()
	 */
	@Override
	public boolean isTextScoreProperty() {
		return isAnnotationPresent(TextScore.class);
	}

	@Override
	public Object[] getEncryptionKeyIds() {

		Encrypted encrypted = findAnnotation(Encrypted.class);
		if(encrypted == null) {
			return null;
		}
		List<Object> target = new ArrayList<>();
		EvaluationContext evaluationContext = ((BasicMongoPersistentEntity)getOwner()). getEvaluationContext(null);
		evaluationContext.setVariable("target", getName());
		for(String keyId : encrypted.keyId()) {
			Expression expression = detectExpression(keyId);
			if(expression == null) {
				try {
					target.add(UUID.fromString(keyId));
				} catch (IllegalArgumentException e) {
					target.add(org.bson.Document.parse("{ val : { $binary : { base64 : '" + keyId + "', subType : '04'} } }").get("val"));
				}
			} else {
				target.add(expression.getValue(evaluationContext));
			}
		}
		return target.toArray();
	}

	private Expression detectExpression(String keyId) {
		Expression expression = new SpelExpressionParser().parseExpression(keyId, ParserContext.TEMPLATE_EXPRESSION);
		return expression instanceof LiteralExpression ? null : expression;
	}
}
