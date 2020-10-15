/*
 * Copyright 2020. the original author or authors.
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

/*
 * Copyright 2020 the original author or authors.
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
package org.springframework.data.util;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PreferredConstructor;
import org.springframework.data.mapping.PreferredConstructorProvider;
import org.springframework.data.mapping.model.AccessorFunctionAware;
import org.springframework.data.mapping.model.EntityInstantiator;
import org.springframework.data.mapping.model.EntiyInstantiatorAware;
import org.springframework.data.mapping.model.PersistentPropertyAccessorFactory;
import org.springframework.data.mapping.model.PersistentPropertyAccessorFactoryProvider;
import org.springframework.data.mapping.model.StaticPropertyAccessorFactory;
import org.springframework.lang.Nullable;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.util.ObjectUtils;

/**
 * @author Christoph Strobl
 * @since 2020/10
 */
public class DomainTypeInformation<S> extends ClassTypeInformation<S>
		implements AnnotationAware, EntiyInstantiatorAware, PreferredConstructorProvider<S>,
		PersistentPropertyAccessorFactoryProvider, AccessorFunctionAware<S> {

	private final Class<S> type;

	@Nullable private final TypeInformation<?> componentType;
	@Nullable private final TypeInformation<?> keyType;

	private DomainTypeInformation<?> superTypeInformation;
	private List<TypeInformation<?>> typeArguments;
	private MultiValueMap<Class<? extends Annotation>, Annotation> annotations;

	private final Fields fields;

	private EntityInstantiator instantiator;
	private PreferredConstructor<S, ?> preferredConstructor;

	public DomainTypeInformation(Class<S> type) {
		this(type, null, null);
	}

	public DomainTypeInformation(Class<S> type, @Nullable TypeInformation<?> componentType,
			@Nullable TypeInformation<?> keyType) {

		super(type, componentType, keyType);
		this.type = type;
		this.componentType = componentType;
		this.keyType = keyType;
		this.typeArguments = computeTypeArguments();
		this.instantiator = computeEntityInstantiator();
		this.preferredConstructor = computePreferredConstructor();
		this.annotations = new LinkedMultiValueMap<>();
		this.fields = new Fields(type);

		computeFields();
		computeAnnotations();
	}

	protected void addField(Field<?, S> field) {
		this.fields.add(field);
	}

	protected List<TypeInformation<?>> computeTypeArguments() {
		return Collections.emptyList();
	}

	protected EntityInstantiator computeEntityInstantiator() {
		return null;
	}

	protected PreferredConstructor<S, ?> computePreferredConstructor() {
		return null;
	}

	@Override
	public PreferredConstructor<S, ?> getPreferredConstructor() {
		return preferredConstructor;
	}

	protected void computeFields() {
		//
	}

	protected void computeAnnotations() {

	}

	protected void addAnnotation(Annotation annotation) {
		this.annotations.add(annotation.annotationType(), annotation);
	}

	public void doWithFields(BiConsumer<String, Field<?, S>> consumer) {
		fields.doWithFields(consumer);
	}

	@Override
	public List<TypeInformation<?>> getParameterTypes(Constructor<?> constructor) {
		return null;
	}

	@Nullable
	@Override
	public TypeInformation<?> getProperty(String property) {

		if (!fields.hasField(property)) {
			return null;
		}

		return fields.getField(property).getTypeInformation();
	}

	@Override
	public boolean isCollectionLike() {
		return false;
	}

	@Override
	public boolean isMap() {
		return false;
	}

	@Nullable
	@Override
	public TypeInformation<?> getMapValueType() {
		return componentType;
	}

	@Override
	public Class<S> getType() {
		return type;
	}

	@Override
	public ClassTypeInformation<?> getRawTypeInformation() {
		return this;
	}

	@Nullable
	@Override
	public TypeInformation<?> getActualType() {
		return componentType != null ? componentType : this;
	}

	@Override
	public TypeInformation<?> getReturnType(Method method) {
		return null;
	}

	@Override
	public List<TypeInformation<?>> getParameterTypes(Method method) {
		return Collections.emptyList();
	}

	@Nullable
	@Override
	public TypeInformation<?> getSuperTypeInformation(Class<?> superType) {
		return superTypeInformation;
	}

	@Override
	public boolean isAssignableFrom(TypeInformation<?> target) {
		return this.type.isAssignableFrom(target.getType());
	}

	@Override
	public List<TypeInformation<?>> getTypeArguments() {
		return typeArguments;
	}

	@Override
	public TypeInformation<? extends S> specialize(ClassTypeInformation<?> type) {
		return null;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		if (!super.equals(o))
			return false;

		DomainTypeInformation<?> that = (DomainTypeInformation<?>) o;

		if (!ObjectUtils.nullSafeEquals(type, that.type)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(componentType, that.componentType)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(keyType, that.keyType)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(superTypeInformation, that.superTypeInformation)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(typeArguments, that.typeArguments)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(fields, that.fields)) {
			return false;
		}
		return ObjectUtils.nullSafeEquals(instantiator, that.instantiator);
	}

	@Override
	public int hashCode() {
		int result = super.hashCode();
		result = 31 * result + ObjectUtils.nullSafeHashCode(type);
		result = 31 * result + ObjectUtils.nullSafeHashCode(componentType);
		result = 31 * result + ObjectUtils.nullSafeHashCode(keyType);
		result = 31 * result + ObjectUtils.nullSafeHashCode(superTypeInformation);
		result = 31 * result + ObjectUtils.nullSafeHashCode(typeArguments);
		result = 31 * result + ObjectUtils.nullSafeHashCode(fields);

		result = 31 * result + ObjectUtils.nullSafeHashCode(instantiator);
		return result;
	}

	@Nullable
	@Override
	public EntityInstantiator getEntityInstantiator() {
		return instantiator;
	}

	@Override
	public List<Annotation> getAnnotations() {

		List<Annotation> all = new ArrayList<>();
		annotations.values().forEach(all::addAll);
		return all;
	}

	@Override
	public boolean hasAnnotation(Class<?> annotationType) {
		return annotations.containsKey(annotationType);
	}

	@Override
	public <T extends Annotation> List<T> findAnnotation(Class<T> annotation) {
		return (List<T>) annotations.getOrDefault(annotation, Collections.emptyList());
	}

	@Nullable
	@Override
	public PersistentPropertyAccessorFactory getPersistentPropertyAccessorFactory() {
		return StaticPropertyAccessorFactory.instance();
	}

	@Override
	public BiFunction<S, Object, S> getSetFunctionFor(String fieldName) {

		Field<Object, S> entityField = fields.getField(fieldName);
		if (entityField == null) {
			return null;
		}

		return entityField.getSetter();
	}

	@Override
	public Function<S, Object> getGetFunctionFor(String fieldName) {

		Field<Object, S> entityField = fields.getField(fieldName);
		if (entityField == null) {
			return null;
		}

		return entityField.getGetter();
	}

	public static class StaticPreferredConstructor extends PreferredConstructor {

		private List<String> args;

		public StaticPreferredConstructor(List<String> args) {
			this.args = args;
		}

		public static StaticPreferredConstructor of(String... args) {
			return new StaticPreferredConstructor(Arrays.asList(args));
		}

		@Override
		public boolean isConstructorParameter(PersistentProperty property) {

			if (args.contains(property.getName())) {
				return true;
			}

			return super.isConstructorParameter(property);
		}

		@Override
		public boolean hasParameters() {
			return !args.isEmpty();
		}
	}
}
