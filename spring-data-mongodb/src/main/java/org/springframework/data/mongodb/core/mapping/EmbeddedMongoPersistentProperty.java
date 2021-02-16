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
package org.springframework.data.mongodb.core.mapping;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;

/**
 * Embedded variant of {@link MongoPersistentProperty}.
 *
 * @author Christoph Strobl
 * @since 3.2
 * @see Embedded
 */
class EmbeddedMongoPersistentProperty implements MongoPersistentProperty {

	private final MongoPersistentProperty delegate;
	private final EmbeddedEntityContext context;

	public EmbeddedMongoPersistentProperty(MongoPersistentProperty delegate, EmbeddedEntityContext context) {

		this.delegate = delegate;
		this.context = context;
	}

	@Override
	public String getFieldName() {

		if (!context.getProperty().isEmbedded()) {
			return delegate.getFieldName();
		}

		return context.getProperty().findAnnotation(Embedded.class).prefix() + delegate.getFieldName();
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
	public boolean isDbReference() {
		return delegate.isDbReference();
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
	@Nullable
	public DBRef getDBRef() {
		return delegate.getDBRef();
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
	public TypeInformation<?> getTypeInformation() {
		return delegate.getTypeInformation();
	}

	@Override
	public Iterable<? extends TypeInformation<?>> getPersistentEntityTypes() {
		return delegate.getPersistentEntityTypes();
	}

	@Override
	@Nullable
	public Method getGetter() {
		return delegate.getGetter();
	}

	@Override
	public Method getRequiredGetter() {
		return delegate.getRequiredGetter();
	}

	@Override
	@Nullable
	public Method getSetter() {
		return delegate.getSetter();
	}

	@Override
	public Method getRequiredSetter() {
		return delegate.getRequiredSetter();
	}

	@Override
	@Nullable
	public Method getWither() {
		return delegate.getWither();
	}

	@Override
	public Method getRequiredWither() {
		return delegate.getRequiredWither();
	}

	@Override
	@Nullable
	public Field getField() {
		return delegate.getField();
	}

	@Override
	public Field getRequiredField() {
		return delegate.getRequiredField();
	}

	@Override
	@Nullable
	public String getSpelExpression() {
		return delegate.getSpelExpression();
	}

	@Override
	@Nullable
	public Association<MongoPersistentProperty> getAssociation() {
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
	public boolean isImmutable() {
		return delegate.isImmutable();
	}

	@Override
	public boolean isAssociation() {
		return delegate.isAssociation();
	}

	@Override
	public boolean isEmbedded() {
		return delegate.isEmbedded();
	}

	@Override
	@Nullable
	public Class<?> getComponentType() {
		return delegate.getComponentType();
	}

	@Override
	public Class<?> getRawType() {
		return delegate.getRawType();
	}

	@Override
	@Nullable
	public Class<?> getMapValueType() {
		return delegate.getMapValueType();
	}

	@Override
	public Class<?> getActualType() {
		return delegate.getActualType();
	}

	@Override
	@Nullable
	public <A extends Annotation> A findAnnotation(Class<A> annotationType) {
		return delegate.findAnnotation(annotationType);
	}

	@Override
	public <A extends Annotation> A getRequiredAnnotation(Class<A> annotationType) throws IllegalStateException {
		return delegate.getRequiredAnnotation(annotationType);
	}

	@Override
	@Nullable
	public <A extends Annotation> A findPropertyOrOwnerAnnotation(Class<A> annotationType) {
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
	@Nullable
	public Class<?> getAssociationTargetType() {
		return delegate.getAssociationTargetType();
	}

	@Override
	public <T> PersistentPropertyAccessor<T> getAccessorForOwner(T owner) {
		return delegate.getAccessorForOwner(owner);
	}
}
