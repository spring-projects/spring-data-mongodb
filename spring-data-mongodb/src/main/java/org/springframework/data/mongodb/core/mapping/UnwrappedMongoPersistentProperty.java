/*
 * Copyright 2021-2025 the original author or authors.
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

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;

import org.jspecify.annotations.Nullable;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.ObjectUtils;

/**
 * Unwrapped variant of {@link MongoPersistentProperty}.
 *
 * @author Christoph Strobl
 * @author Rogério Meneguelli Gatto
 * @since 3.2
 * @see Unwrapped
 */
class UnwrappedMongoPersistentProperty implements MongoPersistentProperty {

	private final MongoPersistentProperty delegate;
	private final UnwrapEntityContext context;

	public UnwrappedMongoPersistentProperty(MongoPersistentProperty delegate, UnwrapEntityContext context) {

		this.delegate = delegate;
		this.context = context;
	}

	@Override
	@SuppressWarnings("NullAway")
	public String getFieldName() {

		if (!context.getProperty().isUnwrapped()) {
			return delegate.getFieldName();
		}

		return context.getProperty().findAnnotation(Unwrapped.class).prefix() + delegate.getFieldName();
	}

	@Override
	@SuppressWarnings("NullAway")
	public boolean hasExplicitFieldName() {
		return delegate.hasExplicitFieldName()
				|| !ObjectUtils.isEmpty(context.getProperty().findAnnotation(Unwrapped.class).prefix());
	}

	@Override
	public Class<?> getFieldType() {
		return delegate.getFieldType();
	}

	@Override
	public int getFieldOrder() {
		return delegate.getFieldOrder();
	}

	@Override
	public boolean writeNullValues() {
		return delegate.writeNullValues();
	}

	@Override
	public boolean isDbReference() {
		return delegate.isDbReference();
	}

	@Override
	public boolean isDocumentReference() {
		return delegate.isDocumentReference();
	}

	@Override
	public boolean isExplicitIdProperty() {
		return delegate.isExplicitIdProperty();
	}

	@Override
	public boolean isLanguageProperty() {
		return delegate.isLanguageProperty();
	}

	@Override
	public boolean isExplicitLanguageProperty() {
		return delegate.isExplicitLanguageProperty();
	}

	@Override
	public boolean isTextScoreProperty() {
		return delegate.isTextScoreProperty();
	}

	@Override
	public @Nullable DBRef getDBRef() {
		return delegate.getDBRef();
	}

	@Override
	public @Nullable DocumentReference getDocumentReference() {
		return delegate.getDocumentReference();
	}

	@Override
	public boolean usePropertyAccess() {
		return delegate.usePropertyAccess();
	}

	@Override
	public boolean hasExplicitWriteTarget() {
		return delegate.hasExplicitWriteTarget();
	}

	@Override
	public PersistentEntity<?, MongoPersistentProperty> getOwner() {
		return delegate.getOwner();
	}

	@Override
	public String getName() {
		return delegate.getName();
	}

	@Override
	public Class<?> getType() {
		return delegate.getType();
	}

	@Override
	@SuppressWarnings("NullAway")
	public MongoField getMongoField() {

		if (!context.getProperty().isUnwrapped()) {
			return delegate.getMongoField();
		}

		return delegate.getMongoField().withPrefix(context.getProperty().findAnnotation(Unwrapped.class).prefix());
	}

	@Override
	public TypeInformation<?> getTypeInformation() {
		return delegate.getTypeInformation();
	}

	@Override
	public Iterable<? extends TypeInformation<?>> getPersistentEntityTypeInformation() {
		return delegate.getPersistentEntityTypeInformation();
	}

	@Override
	public @Nullable Method getGetter() {
		return delegate.getGetter();
	}

	@Override
	public Method getRequiredGetter() {
		return delegate.getRequiredGetter();
	}

	@Override
	public @Nullable Method getSetter() {
		return delegate.getSetter();
	}

	@Override
	public Method getRequiredSetter() {
		return delegate.getRequiredSetter();
	}

	@Override
	public @Nullable Method getWither() {
		return delegate.getWither();
	}

	@Override
	public Method getRequiredWither() {
		return delegate.getRequiredWither();
	}

	@Override
	public @Nullable Field getField() {
		return delegate.getField();
	}

	@Override
	public Field getRequiredField() {
		return delegate.getRequiredField();
	}

	@Override
	public @Nullable String getSpelExpression() {
		return delegate.getSpelExpression();
	}

	@Override
	public @Nullable Association<MongoPersistentProperty> getAssociation() {
		return delegate.getAssociation();
	}

	@Override
	public Association<MongoPersistentProperty> getRequiredAssociation() {
		return delegate.getRequiredAssociation();
	}

	@Override
	public boolean isEntity() {
		return delegate.isEntity();
	}

	@Override
	public boolean isIdProperty() {
		return delegate.isIdProperty();
	}

	@Override
	public boolean isVersionProperty() {
		return delegate.isVersionProperty();
	}

	@Override
	public boolean isCollectionLike() {
		return delegate.isCollectionLike();
	}

	@Override
	public boolean isMap() {
		return delegate.isMap();
	}

	@Override
	public boolean isArray() {
		return delegate.isArray();
	}

	@Override
	public boolean isTransient() {
		return delegate.isTransient();
	}

	@Override
	public boolean isWritable() {
		return delegate.isWritable();
	}

	@Override
	public boolean isReadable() {
		return delegate.isReadable();
	}

	@Override
	public boolean isImmutable() {
		return delegate.isImmutable();
	}

	@Override
	public boolean isAssociation() {
		return delegate.isAssociation();
	}

	@Override
	public boolean isUnwrapped() {
		return delegate.isUnwrapped();
	}

	@Override
	public Collection<Object> getEncryptionKeyIds() {
		return delegate.getEncryptionKeyIds();
	}

	@Override
	public @Nullable Class<?> getComponentType() {
		return delegate.getComponentType();
	}

	@Override
	public Class<?> getRawType() {
		return delegate.getRawType();
	}

	@Override
	public @Nullable Class<?> getMapValueType() {
		return delegate.getMapValueType();
	}

	@Override
	public Class<?> getActualType() {
		return delegate.getActualType();
	}

	@Override
	public <A extends Annotation> @Nullable A findAnnotation(Class<A> annotationType) {
		return delegate.findAnnotation(annotationType);
	}

	@Override
	public <A extends Annotation> A getRequiredAnnotation(Class<A> annotationType) throws IllegalStateException {
		return delegate.getRequiredAnnotation(annotationType);
	}

	@Override
	public <A extends Annotation> @Nullable A findPropertyOrOwnerAnnotation(Class<A> annotationType) {
		return delegate.findPropertyOrOwnerAnnotation(annotationType);
	}

	@Override
	public boolean isAnnotationPresent(Class<? extends Annotation> annotationType) {
		return delegate.isAnnotationPresent(annotationType);
	}

	@Override
	public boolean hasActualTypeAnnotation(Class<? extends Annotation> annotationType) {
		return delegate.hasActualTypeAnnotation(annotationType);
	}

	@Override
	public @Nullable Class<?> getAssociationTargetType() {
		return delegate.getAssociationTargetType();
	}

	@Override
	public @Nullable TypeInformation<?> getAssociationTargetTypeInformation() {
		return delegate.getAssociationTargetTypeInformation();
	}

	@Override
	public <T> PersistentPropertyAccessor<T> getAccessorForOwner(T owner) {
		return delegate.getAccessorForOwner(owner);
	}

	@Override
	public boolean equals(@Nullable Object obj) {

		if (this == obj) {
			return true;
		}

		if (obj == delegate) {
			return true;
		}

		if (obj == null || getClass() != obj.getClass()) {
			return false;
		}

		UnwrappedMongoPersistentProperty that = (UnwrappedMongoPersistentProperty) obj;
		if (!ObjectUtils.nullSafeEquals(delegate, that.delegate)) {
			return false;
		}
		return ObjectUtils.nullSafeEquals(context, that.context);
	}

	@Override
	public int hashCode() {

		int result = ObjectUtils.nullSafeHashCode(delegate);
		result = 31 * result + ObjectUtils.nullSafeHashCode(context);
		return result;
	}
}
