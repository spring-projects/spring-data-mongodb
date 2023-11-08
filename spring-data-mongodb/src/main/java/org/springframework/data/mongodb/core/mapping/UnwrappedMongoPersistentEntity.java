/*
 * Copyright 2021-2023 the original author or authors.
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.springframework.data.mapping.*;
import org.springframework.data.mapping.model.PersistentPropertyAccessorFactory;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.spel.EvaluationContextProvider;
import org.springframework.data.util.Streamable;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;

/**
 * Unwrapped variant of {@link MongoPersistentEntity}.
 *
 * @author Christoph Strobl
 * @since 3.2
 * @see Unwrapped
 */
class UnwrappedMongoPersistentEntity<T> implements MongoPersistentEntity<T> {

	private final UnwrapEntityContext context;
	private final MongoPersistentEntity<T> delegate;

	public UnwrappedMongoPersistentEntity(MongoPersistentEntity<T> delegate, UnwrapEntityContext context) {

		this.context = context;
		this.delegate = delegate;
	}

	@Override
	public String getCollection() {
		return delegate.getCollection();
	}

	@Override
	public String getLanguage() {
		return delegate.getLanguage();
	}

	@Override
	@Nullable
	public MongoPersistentProperty getTextScoreProperty() {
		return delegate.getTextScoreProperty();
	}

	@Override
	public boolean hasTextScoreProperty() {
		return delegate.hasTextScoreProperty();
	}

	@Override
	@Nullable
	public Collation getCollation() {
		return delegate.getCollation();
	}

	@Override
	public boolean hasCollation() {
		return delegate.hasCollation();
	}

	@Override
	public ShardKey getShardKey() {
		return delegate.getShardKey();
	}

	@Override
	public boolean isSharded() {
		return delegate.isSharded();
	}

	@Override
	public String getName() {
		return delegate.getName();
	}

	@Override
	@Nullable
	@Deprecated
	public PreferredConstructor<T, MongoPersistentProperty> getPersistenceConstructor() {
		return delegate.getPersistenceConstructor();
	}

	@Override
	public InstanceCreatorMetadata<MongoPersistentProperty> getInstanceCreatorMetadata() {
		return delegate.getInstanceCreatorMetadata();
	}

	@Override
	public boolean isCreatorArgument(PersistentProperty<?> property) {
		return delegate.isCreatorArgument(property);
	}

	@Override
	public boolean isIdProperty(PersistentProperty<?> property) {
		return delegate.isIdProperty(property);
	}

	@Override
	public boolean isVersionProperty(PersistentProperty<?> property) {
		return delegate.isVersionProperty(property);
	}

	@Override
	@Nullable
	public MongoPersistentProperty getIdProperty() {
		return delegate.getIdProperty();
	}

	@Override
	public MongoPersistentProperty getRequiredIdProperty() {
		return delegate.getRequiredIdProperty();
	}

	@Override
	@Nullable
	public MongoPersistentProperty getVersionProperty() {
		return delegate.getVersionProperty();
	}

	@Override
	public MongoPersistentProperty getRequiredVersionProperty() {
		return delegate.getRequiredVersionProperty();
	}

	@Override
	@Nullable
	public MongoPersistentProperty getPersistentProperty(String name) {
		return wrap(delegate.getPersistentProperty(name));
	}

	@Override
	public MongoPersistentProperty getRequiredPersistentProperty(String name) {

		MongoPersistentProperty persistentProperty = getPersistentProperty(name);
		if (persistentProperty != null) {
			return persistentProperty;
		}

		throw new IllegalStateException(String.format("Required property %s not found for %s", name, getType()));
	}

	@Override
	@Nullable
	public MongoPersistentProperty getPersistentProperty(Class<? extends Annotation> annotationType) {
		return wrap(delegate.getPersistentProperty(annotationType));
	}

	@Override
	public Iterable<MongoPersistentProperty> getPersistentProperties(Class<? extends Annotation> annotationType) {
		return Streamable.of(delegate.getPersistentProperties(annotationType)).stream().map(this::wrap)
				.collect(Collectors.toList());
	}

	@Override
	public boolean hasIdProperty() {
		return delegate.hasIdProperty();
	}

	@Override
	public boolean hasVersionProperty() {
		return delegate.hasVersionProperty();
	}

	@Override
	public Class<T> getType() {
		return delegate.getType();
	}

	@Override
	public Alias getTypeAlias() {
		return delegate.getTypeAlias();
	}

	@Override
	public TypeInformation<T> getTypeInformation() {
		return delegate.getTypeInformation();
	}

	@Override
	public void doWithProperties(PropertyHandler<MongoPersistentProperty> handler) {

		delegate.doWithProperties((PropertyHandler<MongoPersistentProperty>) property -> {
			handler.doWithPersistentProperty(wrap(property));
		});
	}

	@Override
	public void doWithProperties(SimplePropertyHandler handler) {

		delegate.doWithProperties((SimplePropertyHandler) property -> {
			if (property instanceof MongoPersistentProperty mongoPersistentProperty) {
				handler.doWithPersistentProperty(wrap(mongoPersistentProperty));
			} else {
				handler.doWithPersistentProperty(property);
			}
		});
	}

	@Override
	public void doWithAssociations(AssociationHandler<MongoPersistentProperty> handler) {
		delegate.doWithAssociations(handler);
	}

	@Override
	public void doWithAssociations(SimpleAssociationHandler handler) {
		delegate.doWithAssociations(handler);
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
	public <A extends Annotation> boolean isAnnotationPresent(Class<A> annotationType) {
		return delegate.isAnnotationPresent(annotationType);
	}

	@Override
	public <B> PersistentPropertyAccessor<B> getPropertyAccessor(B bean) {
		return delegate.getPropertyAccessor(bean);
	}

	@Override
	public <B> PersistentPropertyPathAccessor<B> getPropertyPathAccessor(B bean) {
		return delegate.getPropertyPathAccessor(bean);
	}

	@Override
	public IdentifierAccessor getIdentifierAccessor(Object bean) {
		return delegate.getIdentifierAccessor(bean);
	}

	@Override
	public boolean isNew(Object bean) {
		return delegate.isNew(bean);
	}

	@Override
	public boolean isImmutable() {
		return delegate.isImmutable();
	}

	@Override
	public boolean requiresPropertyPopulation() {
		return delegate.requiresPropertyPopulation();
	}

	@Override
	public Iterator<MongoPersistentProperty> iterator() {

		List<MongoPersistentProperty> target = new ArrayList<>();
		delegate.iterator().forEachRemaining(it -> target.add(wrap(it)));
		return target.iterator();
	}

	@Override
	public void forEach(Consumer<? super MongoPersistentProperty> action) {
		delegate.forEach(it -> action.accept(wrap(it)));
	}

	@Override
	public Spliterator<MongoPersistentProperty> spliterator() {
		return delegate.spliterator();
	}

	private MongoPersistentProperty wrap(MongoPersistentProperty source) {
		if (source == null) {
			return source;
		}
		return new UnwrappedMongoPersistentProperty(source, context);
	}

	@Override
	public void addPersistentProperty(MongoPersistentProperty property) {

	}

	@Override
	public void addAssociation(Association<MongoPersistentProperty> association) {

	}

	@Override
	public void verify() throws MappingException {

	}

	@Override
	public void setPersistentPropertyAccessorFactory(PersistentPropertyAccessorFactory factory) {

	}

	@Override
	public void setEvaluationContextProvider(EvaluationContextProvider provider) {

	}

	@Override
	public boolean isUnwrapped() {
		return context.getProperty().isUnwrapped();
	}

	@Override
	public Collection<Object> getEncryptionKeyIds() {
		return delegate.getEncryptionKeyIds();
	}
}
