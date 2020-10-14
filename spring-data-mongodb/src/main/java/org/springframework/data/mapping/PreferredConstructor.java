/*
 * Copyright 2011-2020 the original author or authors.
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
package org.springframework.data.mapping;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.util.Lazy;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

/**
 * Value object to encapsulate the constructor to be used when mapping persistent data to objects.
 *
 * @author Oliver Gierke
 * @author Jon Brisbin
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Myeonghyeon Lee
 */
public class PreferredConstructor<T, P extends PersistentProperty<P>> {

	private final Constructor<T> constructor;
	private final List<Parameter<Object, P>> parameters;
	private final Map<PersistentProperty<?>, Boolean> isPropertyParameterCache = new ConcurrentHashMap<>();

	public PreferredConstructor() {
		this.constructor = null;
		this.parameters = Collections.emptyList();
	}

	/**
	 * Creates a new {@link PreferredConstructor} from the given {@link Constructor} and {@link Parameter}s.
	 *
	 * @param constructor must not be {@literal null}.
	 * @param parameters must not be {@literal null}.
	 */
	@SafeVarargs
	public PreferredConstructor(Constructor<T> constructor, Parameter<Object, P>... parameters) {

		Assert.notNull(constructor, "Constructor must not be null!");
		Assert.notNull(parameters, "Parameters must not be null!");

		ReflectionUtils.makeAccessible(constructor);
		this.constructor = constructor;
		this.parameters = Arrays.asList(parameters);
	}

	/**
	 * Returns the underlying {@link Constructor}.
	 *
	 * @return
	 */
	public Constructor<T> getConstructor() {
		return constructor;
	}

	/**
	 * Returns the {@link Parameter}s of the constructor.
	 *
	 * @return
	 */
	public List<Parameter<Object, P>> getParameters() {
		return parameters;
	}

	/**
	 * Returns whether the constructor has {@link Parameter}s.
	 *
	 * @see #isNoArgConstructor()
	 * @return
	 */
	public boolean hasParameters() {
		return !parameters.isEmpty();
	}

	/**
	 * Returns whether the constructor does not have any arguments.
	 *
	 * @see #hasParameters()
	 * @return
	 */
	public boolean isNoArgConstructor() {
		return parameters.isEmpty();
	}

	/**
	 * Returns whether the constructor was explicitly selected (by {@link PersistenceConstructor}).
	 *
	 * @return
	 */
	public boolean isExplicitlyAnnotated() {
		return constructor.isAnnotationPresent(PersistenceConstructor.class);
	}

	/**
	 * Returns whether the given {@link PersistentProperty} is referenced in a constructor argument of the
	 * {@link PersistentEntity} backing this {@link PreferredConstructor}.
	 *
	 * @param property must not be {@literal null}.
	 * @return {@literal true} if the {@link PersistentProperty} is used in the constructor.
	 */
	public boolean isConstructorParameter(PersistentProperty<?> property) {

		Assert.notNull(property, "Property must not be null!");

		Boolean cached = isPropertyParameterCache.get(property);

		if (cached != null) {
			return cached;
		}

		boolean result = false;
		for (Parameter<?, P> parameter : parameters) {
			if (parameter.maps(property)) {
				isPropertyParameterCache.put(property, true);
				result = true;
				break;
			}
		}

		return result;
	}

	/**
	 * Returns whether the given {@link Parameter} is one referring to an enclosing class. That is in case the class this
	 * {@link PreferredConstructor} belongs to is a member class actually. If that's the case the compiler creates a first
	 * constructor argument of the enclosing class type.
	 *
	 * @param parameter must not be {@literal null}.
	 * @return {@literal true} if the {@link PersistentProperty} maps to the enclosing class.
	 */
	public boolean isEnclosingClassParameter(Parameter<?, P> parameter) {

		Assert.notNull(parameter, "Parameter must not be null!");

		if (parameters.isEmpty() || !parameter.isEnclosingClassParameter()) {
			return false;
		}

		return parameters.get(0).equals(parameter);
	}

	/**
	 * Value object to represent constructor parameters.
	 *
	 * @param <T> the type of the parameter
	 * @author Oliver Gierke
	 */
	public static class Parameter<T, P extends PersistentProperty<P>> {

		private final @Nullable String name;
		private final TypeInformation<T> type;
		private final String key;
		private final @Nullable PersistentEntity<T, P> entity;

		private final Lazy<Boolean> enclosingClassCache;
		private final Lazy<Boolean> hasSpelExpression;

		/**
		 * Creates a new {@link Parameter} with the given name, {@link TypeInformation} as well as an array of
		 * {@link Annotation}s. Will inspect the annotations for an {@link Value} annotation to lookup a key or an SpEL
		 * expression to be evaluated.
		 *
		 * @param name the name of the parameter, can be {@literal null}
		 * @param type must not be {@literal null}
		 * @param annotations must not be {@literal null} but can be empty
		 * @param entity must not be {@literal null}.
		 */
		public Parameter(@Nullable String name, TypeInformation<T> type, Annotation[] annotations,
				@Nullable PersistentEntity<T, P> entity) {

			Assert.notNull(type, "Type must not be null!");
			Assert.notNull(annotations, "Annotations must not be null!");

			this.name = name;
			this.type = type;
			this.key = getValue(annotations);
			this.entity = entity;

			this.enclosingClassCache = Lazy.of(() -> {

				if (entity == null) {
					throw new IllegalStateException();
				}

				Class<T> owningType = entity.getType();
				return owningType.isMemberClass() && type.getType().equals(owningType.getEnclosingClass());
			});

			this.hasSpelExpression = Lazy.of(() -> StringUtils.hasText(getSpelExpression()));
		}

		@Nullable
		private static String getValue(Annotation[] annotations) {

			return Arrays.stream(annotations)//
					.filter(it -> it.annotationType() == Value.class)//
					.findFirst().map(it -> ((Value) it).value())//
					.filter(StringUtils::hasText).orElse(null);
		}

		/**
		 * Returns the name of the parameter.
		 *
		 * @return
		 */
		@Nullable
		public String getName() {
			return name;
		}

		/**
		 * Returns the {@link TypeInformation} of the parameter.
		 *
		 * @return
		 */
		public TypeInformation<T> getType() {
			return type;
		}

		/**
		 * Returns the raw resolved type of the parameter.
		 *
		 * @return
		 */
		public Class<T> getRawType() {
			return type.getType();
		}

		/**
		 * Returns the key to be used when looking up a source data structure to populate the actual parameter value.
		 *
		 * @return
		 */
		public String getSpelExpression() {
			return key;
		}

		/**
		 * Returns whether the constructor parameter is equipped with a SpEL expression.
		 *
		 * @return
		 */
		public boolean hasSpelExpression() {
			return this.hasSpelExpression.get();
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(Object o) {

			if (this == o) {
				return true;
			}

			if (!(o instanceof Parameter)) {
				return false;
			}

			Parameter<?, ?> parameter = (Parameter<?, ?>) o;

			if (!ObjectUtils.nullSafeEquals(name, parameter.name)) {
				return false;
			}

			if (!ObjectUtils.nullSafeEquals(type, parameter.type)) {
				return false;
			}

			if (!ObjectUtils.nullSafeEquals(key, parameter.key)) {
				return false;
			}

			return ObjectUtils.nullSafeEquals(entity, parameter.entity);
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode() {
			int result = ObjectUtils.nullSafeHashCode(name);
			result = 31 * result + ObjectUtils.nullSafeHashCode(type);
			result = 31 * result + ObjectUtils.nullSafeHashCode(key);
			result = 31 * result + ObjectUtils.nullSafeHashCode(entity);
			return result;
		}

		/**
		 * Returns whether the {@link Parameter} maps the given {@link PersistentProperty}.
		 *
		 * @param property
		 * @return
		 */
		boolean maps(PersistentProperty<?> property) {

			PersistentEntity<T, P> entity = this.entity;
			String name = this.name;

			P referencedProperty = entity == null ? null : name == null ? null : entity.getPersistentProperty(name);

			return property.equals(referencedProperty);
		}

		private boolean isEnclosingClassParameter() {
			return enclosingClassCache.get();
		}
	}
}
