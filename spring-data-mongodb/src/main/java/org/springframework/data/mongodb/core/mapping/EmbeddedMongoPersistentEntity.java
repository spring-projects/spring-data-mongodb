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
import java.util.ArrayList;
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
 * @author Christoph Strobl
 * @since 2020/12
 */
public class EmbeddedMongoPersistentEntity<T> implements MongoPersistentEntity<T> {

	private EmbeddedEntityContext context;
	private MongoPersistentEntity<T> delegate;

	public EmbeddedMongoPersistentEntity(MongoPersistentEntity<T> delegate, EmbeddedEntityContext context) {

		this.context = context;
		this.delegate = delegate;
	}

	public String getCollection() {
		return delegate.getCollection();
	}

	public String getLanguage() {
		return delegate.getLanguage();
	}

	@Nullable
	public MongoPersistentProperty getTextScoreProperty() {
		return delegate.getTextScoreProperty();
	}

	public boolean hasTextScoreProperty() {
		return delegate.hasTextScoreProperty();
	}

	@Nullable
	public Collation getCollation() {
		return delegate.getCollation();
	}

	public boolean hasCollation() {
		return delegate.hasCollation();
	}

	public ShardKey getShardKey() {
		return delegate.getShardKey();
	}

	public boolean isSharded() {
		return delegate.isSharded();
	}

	public String getName() {
		return delegate.getName();
	}

	@Nullable
	public PreferredConstructor<T, MongoPersistentProperty> getPersistenceConstructor() {
		return delegate.getPersistenceConstructor();
	}

	public boolean isConstructorArgument(PersistentProperty<?> property) {
		return delegate.isConstructorArgument(property);
	}

	public boolean isIdProperty(PersistentProperty<?> property) {
		return delegate.isIdProperty(property);
	}

	public boolean isVersionProperty(PersistentProperty<?> property) {
		return delegate.isVersionProperty(property);
	}

	@Nullable
	public MongoPersistentProperty getIdProperty() {
		return delegate.getIdProperty();
	}

	public MongoPersistentProperty getRequiredIdProperty() {
		return delegate.getRequiredIdProperty();
	}

	@Nullable
	public MongoPersistentProperty getVersionProperty() {
		return delegate.getVersionProperty();
	}

	public MongoPersistentProperty getRequiredVersionProperty() {
		return delegate.getRequiredVersionProperty();
	}

	@Nullable
	public MongoPersistentProperty getPersistentProperty(String name) {
		return wrap(delegate.getPersistentProperty(name));
	}

	public MongoPersistentProperty getRequiredPersistentProperty(String name) {

		MongoPersistentProperty persistentProperty = getPersistentProperty(name);
		if (persistentProperty != null) {
			return persistentProperty;
		}

		throw new RuntimeException(":kladjnf");
	}

	@Nullable
	public MongoPersistentProperty getPersistentProperty(Class<? extends Annotation> annotationType) {
		return wrap(delegate.getPersistentProperty(annotationType));
	}

	public Iterable<MongoPersistentProperty> getPersistentProperties(Class<? extends Annotation> annotationType) {
		return Streamable.of(delegate.getPersistentProperties(annotationType)).stream().map(this::wrap)
				.collect(Collectors.toList());
	}

	public boolean hasIdProperty() {
		return delegate.hasIdProperty();
	}

	public boolean hasVersionProperty() {
		return delegate.hasVersionProperty();
	}

	public Class<T> getType() {
		return delegate.getType();
	}

	public Alias getTypeAlias() {
		return delegate.getTypeAlias();
	}

	public TypeInformation<T> getTypeInformation() {
		return delegate.getTypeInformation();
	}

	public void doWithProperties(PropertyHandler<MongoPersistentProperty> handler) {

		delegate.doWithProperties((PropertyHandler<MongoPersistentProperty>) property -> {
			handler.doWithPersistentProperty(wrap(property));
		});
	}

	public void doWithProperties(SimplePropertyHandler handler) {

		delegate.doWithProperties((SimplePropertyHandler) property -> {
			if (property instanceof MongoPersistentProperty) {
				handler.doWithPersistentProperty(wrap((MongoPersistentProperty) property));
			} else {
				handler.doWithPersistentProperty(property);
			}
		});
	}

	public void doWithAssociations(AssociationHandler<MongoPersistentProperty> handler) {
		delegate.doWithAssociations(handler);
	}

	public void doWithAssociations(SimpleAssociationHandler handler) {
		delegate.doWithAssociations(handler);
	}

	@Nullable
	public <A extends Annotation> A findAnnotation(Class<A> annotationType) {
		return delegate.findAnnotation(annotationType);
	}

	public <A extends Annotation> A getRequiredAnnotation(Class<A> annotationType) throws IllegalStateException {
		return delegate.getRequiredAnnotation(annotationType);
	}

	public <A extends Annotation> boolean isAnnotationPresent(Class<A> annotationType) {
		return delegate.isAnnotationPresent(annotationType);
	}

	public <B> PersistentPropertyAccessor<B> getPropertyAccessor(B bean) {
		return delegate.getPropertyAccessor(bean);
	}

	public <B> PersistentPropertyPathAccessor<B> getPropertyPathAccessor(B bean) {
		return delegate.getPropertyPathAccessor(bean);
	}

	public IdentifierAccessor getIdentifierAccessor(Object bean) {
		return delegate.getIdentifierAccessor(bean);
	}

	public boolean isNew(Object bean) {
		return delegate.isNew(bean);
	}

	public boolean isImmutable() {
		return delegate.isImmutable();
	}

	public boolean requiresPropertyPopulation() {
		return delegate.requiresPropertyPopulation();
	}

	public Iterator<MongoPersistentProperty> iterator() {

		List<MongoPersistentProperty> target = new ArrayList<>();
		delegate.iterator().forEachRemaining(it -> target.add(wrap(it)));
		return target.iterator();
	}

	public void forEach(Consumer<? super MongoPersistentProperty> action) {
		delegate.forEach(it -> action.accept(wrap(it)));
	}

	public Spliterator<MongoPersistentProperty> spliterator() {
		return delegate.spliterator();
	}

	private MongoPersistentProperty wrap(MongoPersistentProperty source) {
		if (source == null) {
			return source;
		}
		return new EmbeddedMongoPersistentProperty(source, context);
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
	public boolean isEmbedded() {
		return context.getProperty().isEmbedded();
	}
}
