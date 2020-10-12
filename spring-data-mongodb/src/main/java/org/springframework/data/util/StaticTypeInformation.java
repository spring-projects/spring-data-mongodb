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

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.springframework.data.mapping.PreferredConstructor;
import org.springframework.data.mapping.model.EntityInstantiator;
import org.springframework.lang.Nullable;
import org.springframework.util.ObjectUtils;

/**
 * @author Christoph Strobl
 * @since 2020/10
 */
public class StaticTypeInformation<S> extends ClassTypeInformation<S> {

	private final Class<S> type;

	@Nullable private final TypeInformation<?> componentType;
	@Nullable private final TypeInformation<?> keyType;

	private StaticTypeInformation<?> superTypeInformation;
	private List<TypeInformation<?>> typeArguments;
	private final Map<String, TypeInformation<?>> properties;
	private final Map<String, BiFunction<S,Object,S>> setter;
	private final Map<String, Function<S,Object>> getter;

	private EntityInstantiator instantiator;

	public StaticTypeInformation(Class<S> type) {
		this(type, null, null);
	}

	public StaticTypeInformation(Class<S> type, @Nullable TypeInformation<?> componentType,
			@Nullable TypeInformation<?> keyType) {

		super(type, componentType, keyType);
		this.type = type;
		this.componentType = componentType;
		this.keyType = keyType;
		this.properties = computePropertiesMap();
		this.typeArguments = computeTypeArguments();
		this.instantiator = computeEntityInstantiator();
		this.setter = computeSetter();
		this.getter = computeGetter();
	}

	protected Map<String, TypeInformation<?>> computePropertiesMap() {
		return Collections.emptyMap();
	};

	protected List<TypeInformation<?>> computeTypeArguments() {
		return Collections.emptyList();
	}

	protected EntityInstantiator computeEntityInstantiator() {
		return null;
	}

	protected Map<String, BiFunction<S,Object,S>> computeSetter() {
		return Collections.emptyMap();
	}

	protected Map<String, Function<S,Object>> computeGetter() {
		return Collections.emptyMap();
	}

	public Map<String, TypeInformation<?>> getProperties() {
		return properties;
	}

	public Map<String, BiFunction<S, Object, S>> getSetter() {
		return setter;
	}

	public Map<String, Function<S, Object>> getGetter() {
		return getter;
	}

	public EntityInstantiator getInstantiator() {
		return instantiator;
	}

	@Override
	public List<TypeInformation<?>> getParameterTypes(Constructor<?> constructor) {
		return null;
	}

	@Nullable
	@Override
	public TypeInformation<?> getProperty(String property) {
		return properties.get(property);
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
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		if (!super.equals(o)) return false;

		StaticTypeInformation<?> that = (StaticTypeInformation<?>) o;

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
		if (!ObjectUtils.nullSafeEquals(properties, that.properties)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(setter, that.setter)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(getter, that.getter)) {
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
		result = 31 * result + ObjectUtils.nullSafeHashCode(properties);
		result = 31 * result + ObjectUtils.nullSafeHashCode(setter);
		result = 31 * result + ObjectUtils.nullSafeHashCode(getter);
		result = 31 * result + ObjectUtils.nullSafeHashCode(instantiator);
		return result;
	}

	PreferredConstructor
}
