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
 * @author Christoph Strobl
 * @since 2020/12
 */
public class EmbeddedMongoPersistentProperty implements MongoPersistentProperty {

	private final MongoPersistentProperty delegate;
	private final EmbeddedEntityContext context;

	public EmbeddedMongoPersistentProperty(MongoPersistentProperty delegate, EmbeddedEntityContext context) {

		this.delegate = delegate;
		this.context = context;
	}

	public String getFieldName() {

		if (!context.getProperty().isEmbedded()) {
			return delegate.getFieldName();
		}

		return context.getProperty().findAnnotation(Embedded.class).prefix() + delegate.getFieldName();
	}

	public Class<?> getFieldType() {
		return delegate.getFieldType();
	}

	public int getFieldOrder() {
		return delegate.getFieldOrder();
	}

	public boolean isDbReference() {
		return delegate.isDbReference();
	}

	public boolean isExplicitIdProperty() {
		return delegate.isExplicitIdProperty();
	}

	public boolean isLanguageProperty() {
		return delegate.isLanguageProperty();
	}

	public boolean isExplicitLanguageProperty() {
		return delegate.isExplicitLanguageProperty();
	}

	public boolean isTextScoreProperty() {
		return delegate.isTextScoreProperty();
	}

	@Nullable
	public DBRef getDBRef() {
		return delegate.getDBRef();
	}

	public boolean usePropertyAccess() {
		return delegate.usePropertyAccess();
	}

	public boolean hasExplicitWriteTarget() {
		return delegate.hasExplicitWriteTarget();
	}

	public PersistentEntity<?, MongoPersistentProperty> getOwner() {
		return delegate.getOwner();
	}

	public String getName() {
		return delegate.getName();
	}

	public Class<?> getType() {
		return delegate.getType();
	}

	public TypeInformation<?> getTypeInformation() {
		return delegate.getTypeInformation();
	}

	public Iterable<? extends TypeInformation<?>> getPersistentEntityTypes() {
		return delegate.getPersistentEntityTypes();
	}

	@Nullable
	public Method getGetter() {
		return delegate.getGetter();
	}

	public Method getRequiredGetter() {
		return delegate.getRequiredGetter();
	}

	@Nullable
	public Method getSetter() {
		return delegate.getSetter();
	}

	public Method getRequiredSetter() {
		return delegate.getRequiredSetter();
	}

	@Nullable
	public Method getWither() {
		return delegate.getWither();
	}

	public Method getRequiredWither() {
		return delegate.getRequiredWither();
	}

	@Nullable
	public Field getField() {
		return delegate.getField();
	}

	public Field getRequiredField() {
		return delegate.getRequiredField();
	}

	@Nullable
	public String getSpelExpression() {
		return delegate.getSpelExpression();
	}

	@Nullable
	public Association<MongoPersistentProperty> getAssociation() {
		return delegate.getAssociation();
	}

	public Association<MongoPersistentProperty> getRequiredAssociation() {
		return delegate.getRequiredAssociation();
	}

	public boolean isEntity() {
		return delegate.isEntity();
	}

	public boolean isIdProperty() {
		return delegate.isIdProperty();
	}

	public boolean isVersionProperty() {
		return delegate.isVersionProperty();
	}

	public boolean isCollectionLike() {
		return delegate.isCollectionLike();
	}

	public boolean isMap() {
		return delegate.isMap();
	}

	public boolean isArray() {
		return delegate.isArray();
	}

	public boolean isTransient() {
		return delegate.isTransient();
	}

	public boolean isWritable() {
		return delegate.isWritable();
	}

	public boolean isImmutable() {
		return delegate.isImmutable();
	}

	public boolean isAssociation() {
		return delegate.isAssociation();
	}

	public boolean isEmbedded() {
		return delegate.isEmbedded();
	}

	public boolean isNullable() {
		return delegate.isNullable();
	}

	@Nullable
	public Class<?> getComponentType() {
		return delegate.getComponentType();
	}

	public Class<?> getRawType() {
		return delegate.getRawType();
	}

	@Nullable
	public Class<?> getMapValueType() {
		return delegate.getMapValueType();
	}

	public Class<?> getActualType() {
		return delegate.getActualType();
	}

	@Nullable
	public <A extends Annotation> A findAnnotation(Class<A> annotationType) {
		return delegate.findAnnotation(annotationType);
	}

	public <A extends Annotation> A getRequiredAnnotation(Class<A> annotationType) throws IllegalStateException {
		return delegate.getRequiredAnnotation(annotationType);
	}

	@Nullable
	public <A extends Annotation> A findPropertyOrOwnerAnnotation(Class<A> annotationType) {
		return delegate.findPropertyOrOwnerAnnotation(annotationType);
	}

	public boolean isAnnotationPresent(Class<? extends Annotation> annotationType) {
		return delegate.isAnnotationPresent(annotationType);
	}

	public boolean hasActualTypeAnnotation(Class<? extends Annotation> annotationType) {
		return delegate.hasActualTypeAnnotation(annotationType);
	}

	@Nullable
	public Class<?> getAssociationTargetType() {
		return delegate.getAssociationTargetType();
	}

	public <T> PersistentPropertyAccessor<T> getAccessorForOwner(T owner) {
		return delegate.getAccessorForOwner(owner);
	}
}
