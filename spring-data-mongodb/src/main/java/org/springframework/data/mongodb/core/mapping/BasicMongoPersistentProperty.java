/*
 * Copyright 2011-2023 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.model.AnnotationBasedPersistentProperty;
import org.springframework.data.mapping.model.FieldNamingStrategy;
import org.springframework.data.mapping.model.Property;
import org.springframework.data.mapping.model.PropertyNameFieldNamingStrategy;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.mongodb.core.mapping.FieldName.Type;
import org.springframework.data.mongodb.core.mapping.MongoField.MongoFieldBuilder;
import org.springframework.data.mongodb.util.encryption.EncryptionUtils;
import org.springframework.data.util.Lazy;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.lang.Nullable;
import org.springframework.util.ObjectUtils;
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

	private static final Log LOG = LogFactory.getLog(BasicMongoPersistentProperty.class);

	public static final String ID_FIELD_NAME = FieldName.ID.name();
	private static final String LANGUAGE_FIELD_NAME = "language";
	private static final Set<String> SUPPORTED_ID_PROPERTY_NAMES = Set.of("id", ID_FIELD_NAME);

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
	}

	/**
	 * Also considers fields as id that are of supported id type and name.
	 *
	 * @see #SUPPORTED_ID_PROPERTY_NAMES
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

	@Override
	public boolean isExplicitIdProperty() {
		return super.isIdProperty();
	}

	/**
	 * Returns the key to be used to store the value of the property inside a Mongo {@link org.bson.Document}.
	 *
	 * @return
	 */
	@Override
	public String getFieldName() {
		return getMongoField().getName().name();
	}

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

		FieldType fieldType = getMongoField().getFieldType();
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
	@Override
	public boolean hasExplicitFieldName() {
		return StringUtils.hasText(getAnnotatedFieldName());
	}

	@Nullable
	private String getAnnotatedFieldName() {

		org.springframework.data.mongodb.core.mapping.Field annotation = findAnnotation(
				org.springframework.data.mongodb.core.mapping.Field.class);

		return annotation != null ? annotation.value() : null;
	}

	@Override
	public int getFieldOrder() {
		return getMongoField().getOrder();
	}

	@Override
	public boolean writeNullValues() {

		org.springframework.data.mongodb.core.mapping.Field annotation = findAnnotation(
				org.springframework.data.mongodb.core.mapping.Field.class);

		return annotation != null && annotation.write() == Field.Write.ALWAYS;
	}

	@Override
	protected Association<MongoPersistentProperty> createAssociation() {
		return new Association<>(this, null);
	}

	@Override
	public boolean isDbReference() {
		return isAnnotationPresent(DBRef.class);
	}

	@Override
	public boolean isDocumentReference() {
		return isAnnotationPresent(DocumentReference.class);
	}

	@Override
	@Nullable
	public DBRef getDBRef() {
		return findAnnotation(DBRef.class);
	}

	@Nullable
	@Override
	public DocumentReference getDocumentReference() {
		return findAnnotation(DocumentReference.class);
	}

	@Override
	public boolean isLanguageProperty() {
		return getFieldName().equals(LANGUAGE_FIELD_NAME) || isExplicitLanguageProperty();
	}

	@Override
	public boolean isExplicitLanguageProperty() {
		return isAnnotationPresent(Language.class);
	}

	@Override
	public boolean isTextScoreProperty() {
		return isAnnotationPresent(TextScore.class);
	}

	/**
	 * Obtain the {@link EvaluationContext} for a specific root object.
	 *
	 * @param rootObject can be {@literal null}.
	 * @return never {@literal null}.
	 * @since 3.3
	 */
	public EvaluationContext getEvaluationContext(@Nullable Object rootObject) {

		if (getOwner() instanceof BasicMongoPersistentEntity mongoPersistentEntity) {
			return mongoPersistentEntity.getEvaluationContext(rootObject);
		}
		return rootObject != null ? new StandardEvaluationContext(rootObject) : new StandardEvaluationContext();
	}

	@Override
	public MongoField getMongoField() {
		return doGetMongoField();
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
			ctx.setVariable("target", getOwner().getType().getSimpleName() + "." + getName());
			return ctx;
		});

		List<Object> target = new ArrayList<>();
		for (String keyId : encrypted.keyId()) {
			target.add(EncryptionUtils.resolveKeyId(keyId, evaluationContext));
		}
		return target;
	}

	protected MongoField doGetMongoField() {

		MongoFieldBuilder builder = MongoField.builder();
		if (isAnnotationPresent(Field.class) && Type.KEY.equals(findAnnotation(Field.class).nameType())) {
			builder.name(doGetFieldName());
		} else {
			builder.path(doGetFieldName());
		}
		builder.fieldType(doGetFieldType());
		builder.order(doGetFieldOrder());
		return builder.build();
	}

	private String doGetFieldName() {

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
			throw new MappingException(String.format("Invalid (null or empty) field name returned for property %s by %s",
					this, fieldNamingStrategy.getClass()));
		}

		return fieldName;
	}

	private FieldType doGetFieldType() {

		Field fieldAnnotation = findAnnotation(Field.class);
		return fieldAnnotation != null ? fieldAnnotation.targetType() : FieldType.IMPLICIT;
	}

	private int doGetFieldOrder() {

		Field annotation = findAnnotation(Field.class);
		return annotation != null ? annotation.order() : Integer.MAX_VALUE;
	}

	protected void validate() {

		if (isIdProperty() && hasExplicitFieldName()) {

			String annotatedName = getAnnotatedFieldName();
			if (!ID_FIELD_NAME.equals(annotatedName)) {
				if(LOG.isWarnEnabled()) {
					LOG.warn(String.format(
							"Customizing field name for id property '%s.%s' is not allowed; Custom name ('%s') will not be considered",
							getOwner().getName(), getName(), annotatedName));
				}
			}
		}
	}

}
