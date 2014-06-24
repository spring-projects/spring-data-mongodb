/*
 * Copyright 2014 the original author or authors.
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

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.mapping.AssociationHandler;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PreferredConstructor;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.SimpleAssociationHandler;
import org.springframework.data.mapping.SimplePropertyHandler;
import org.springframework.data.util.TypeInformation;

/**
 * Trivial dummy implementation of {@link MongoPersistentEntity} to be used in tests.
 * 
 * @author Christoph Strobl
 * @param <T>
 */
public class MongoPersistentEntityTestDummy<T> implements MongoPersistentEntity<T> {

	private Map<Class<?>, Annotation> annotations = new HashMap<Class<?>, Annotation>();
	private Collection<MongoPersistentProperty> properties = new ArrayList<MongoPersistentProperty>();
	private String collection;
	private String name;
	private Class<T> type;

	@Override
	public String getName() {
		return name;
	}

	@Override
	public PreferredConstructor<T, MongoPersistentProperty> getPersistenceConstructor() {
		return null;
	}

	@Override
	public boolean isConstructorArgument(PersistentProperty<?> property) {
		return false;
	}

	@Override
	public boolean isIdProperty(PersistentProperty<?> property) {
		return property != null ? property.isIdProperty() : false;
	}

	@Override
	public boolean isVersionProperty(PersistentProperty<?> property) {
		return property != null ? property.isIdProperty() : false;
	}

	@Override
	public MongoPersistentProperty getIdProperty() {
		return getPersistentProperty(Id.class);
	}

	@Override
	public MongoPersistentProperty getVersionProperty() {
		return getPersistentProperty(Version.class);
	}

	@Override
	public MongoPersistentProperty getPersistentProperty(String name) {

		for (MongoPersistentProperty p : this.properties) {
			if (p.getName().equals(name)) {
				return p;
			}
		}
		return null;
	}

	@Override
	public MongoPersistentProperty getPersistentProperty(Class<? extends Annotation> annotationType) {

		for (MongoPersistentProperty p : this.properties) {
			if (p.isAnnotationPresent(annotationType)) {
				return p;
			}
		}
		return null;
	}

	@Override
	public boolean hasIdProperty() {
		return false;
	}

	@Override
	public boolean hasVersionProperty() {
		return getVersionProperty() != null;
	}

	@Override
	public Class<T> getType() {
		return this.type;
	}

	@Override
	public Object getTypeAlias() {
		return null;
	}

	@Override
	public TypeInformation<T> getTypeInformation() {
		return null;
	}

	@Override
	public void doWithProperties(PropertyHandler<MongoPersistentProperty> handler) {

		for (MongoPersistentProperty p : this.properties) {
			handler.doWithPersistentProperty(p);
		}
	}

	@Override
	public void doWithProperties(SimplePropertyHandler handler) {

		for (MongoPersistentProperty p : this.properties) {
			handler.doWithPersistentProperty(p);
		}
	}

	@Override
	public void doWithAssociations(AssociationHandler<MongoPersistentProperty> handler) {

	}

	@Override
	public void doWithAssociations(SimpleAssociationHandler handler) {

	}

	@SuppressWarnings("unchecked")
	@Override
	public <A extends Annotation> A findAnnotation(Class<A> annotationType) {
		return (A) this.annotations.get(annotationType);
	}

	@Override
	public String getCollection() {
		return this.collection;
	}

	/**
	 * Simple builder to create {@link MongoPersistentEntityTestDummy} with defined properties.
	 * 
	 * @author Christoph Strobl
	 * @param <T>
	 */
	public static class MongoPersistentEntityDummyBuilder<T> {

		private MongoPersistentEntityTestDummy<T> instance;

		private MongoPersistentEntityDummyBuilder(Class<T> type) {
			this.instance = new MongoPersistentEntityTestDummy<T>();
			this.instance.type = type;
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		public static <T> MongoPersistentEntityDummyBuilder<T> forClass(Class<T> type) {
			return new MongoPersistentEntityDummyBuilder(type);
		}

		public MongoPersistentEntityDummyBuilder<T> withName(String name) {
			this.instance.name = name;
			return this;
		}

		public MongoPersistentEntityDummyBuilder<T> and(MongoPersistentProperty property) {
			this.instance.properties.add(property);
			return this;
		}

		public MongoPersistentEntityDummyBuilder<T> withCollection(String collection) {
			this.instance.collection = collection;
			return this;
		}

		public MongoPersistentEntityDummyBuilder<T> and(Annotation annotation) {
			this.instance.annotations.put(annotation.annotationType(), annotation);
			return this;
		}

		public MongoPersistentEntityTestDummy<T> build() {
			return this.instance;
		}

	}

	@Override
	public String getLanguage() {
		return null;
	}
}
