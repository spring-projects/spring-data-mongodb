/*
 * Copyright 2016-2020 the original author or authors.
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
package org.springframework.data.mapping.model;

import java.beans.FeatureDescriptor;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.springframework.data.util.Lazy;
import org.springframework.data.util.Optionals;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

/**
 * Value object to abstract the concept of a property backed by a {@link Field} and / or a {@link PropertyDescriptor}.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class Property {

	private @Nullable TypeInformation<?> typeInformation;
	private final Optional<Field> field;
	private final Optional<PropertyDescriptor> descriptor;

	private final Class<?> rawType;
	private final Lazy<Integer> hashCode;
	private final Optional<Method> getter;
	private final Optional<Method> setter;

	private final Lazy<String> name;
	private final Lazy<String> toString;
	private final Lazy<Optional<Method>> wither;

	private Property(String name, TypeInformation<?> typeInformation) {

		this.typeInformation = typeInformation;
		this.field = Optional.empty();
		this.descriptor = Optional.empty();

		this.rawType = typeInformation.getType();
		this.hashCode = Lazy.of( () -> typeInformation.hashCode() + name.hashCode());
		this.getter = Optional.empty();
		this.setter = Optional.empty();
		this.name = Lazy.of(name);
		this.toString = Lazy.of(() ->  typeInformation.toString() + name);
		this.wither = Lazy.of(() -> findWither(typeInformation, getName(), getType()));
	}

	private Property(TypeInformation<?> type, Optional<Field> field, Optional<PropertyDescriptor> descriptor) {

		Assert.notNull(type, "Type must not be null!");
		Assert.isTrue(Optionals.isAnyPresent(field, descriptor), "Either field or descriptor has to be given!");

		this.field = field;
		this.descriptor = descriptor;

		this.rawType = withFieldOrDescriptor( //
				it -> type.getRequiredProperty(it.getName()).getType(), //
				it -> type.getRequiredProperty(it.getName()).getType() //
		);
		this.hashCode = Lazy.of(() -> withFieldOrDescriptor(Object::hashCode));
		this.name = Lazy.of(() -> withFieldOrDescriptor(Field::getName, FeatureDescriptor::getName));
		this.toString = Lazy.of(() -> withFieldOrDescriptor(Object::toString,
				it -> String.format("%s.%s", type.getType().getName(), it.getDisplayName())));

		this.getter = descriptor.map(PropertyDescriptor::getReadMethod)//
				.filter(it -> getType() != null)//
				.filter(it -> getType().isAssignableFrom(type.getReturnType(it).getType()));

		this.setter = descriptor.map(PropertyDescriptor::getWriteMethod)//
				.filter(it -> getType() != null)//
				.filter(it -> type.getParameterTypes(it).get(0).getType().isAssignableFrom(getType()));

		this.wither = Lazy.of(() -> findWither(type, getName(), getType()));
	}

	/**
	 * Creates a new {@link Property} backed by the given field.
	 *
	 * @param type the owning type, must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @return
	 */
	public static Property of(TypeInformation<?> type, Field field) {

		Assert.notNull(field, "Field must not be null!");

		return new Property(type, Optional.of(field), Optional.empty());
	}

	/**
	 * Creates a new {@link Property} backed by the given {@link Field} and {@link PropertyDescriptor}.
	 *
	 * @param type the owning type, must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @param descriptor must not be {@literal null}.
	 * @return
	 */
	public static Property of(TypeInformation<?> type, Field field, PropertyDescriptor descriptor) {

		Assert.notNull(field, "Field must not be null!");
		Assert.notNull(descriptor, "PropertyDescriptor must not be null!");

		return new Property(type, Optional.of(field), Optional.of(descriptor));
	}

	/**
	 * Creates a new {@link Property} backed by the given {@link Field} and {@link PropertyDescriptor}.
	 *
	 * @param type the owning type, must not be {@literal null}.
	 * @param field must not be {@literal null}.
	 * @param descriptor must not be {@literal null}.
	 * @return
	 */
	public static Property of(TypeInformation<?> type, String name) {
		return new Property(name, type);
	}

	/**
	 * Creates a new {@link Property} for the given {@link PropertyDescriptor}. The creation might fail if the given
	 * property is not representing a proper property.
	 *
	 * @param type the owning type, must not be {@literal null}.
	 * @param descriptor must not be {@literal null}.
	 * @return
	 * @see #supportsStandalone(PropertyDescriptor)
	 */
	public static Property of(TypeInformation<?> type, PropertyDescriptor descriptor) {

		Assert.notNull(descriptor, "PropertyDescriptor must not be null!");

		return new Property(type, Optional.empty(), Optional.of(descriptor));
	}

	/**
	 * Returns whether the given {@link PropertyDescriptor} is supported in for standalone creation of a {@link Property}
	 * instance.
	 *
	 * @param descriptor
	 * @return
	 */
	public static boolean supportsStandalone(PropertyDescriptor descriptor) {

		Assert.notNull(descriptor, "PropertyDescriptor must not be null!");

		return descriptor.getPropertyType() != null;
	}

	/**
	 * Returns whether the property is backed by a field.
	 *
	 * @return
	 */
	public boolean isFieldBacked() {
		return field.isPresent();
	}

	/**
	 * Returns the getter of the property if available and if it matches the type of the property.
	 *
	 * @return will never be {@literal null}.
	 */
	public Optional<Method> getGetter() {
		return getter;
	}

	/**
	 * Returns the setter of the property if available and if its first (only) parameter matches the type of the property.
	 *
	 * @return will never be {@literal null}.
	 */
	public Optional<Method> getSetter() {
		return setter;
	}

	/**
	 * Returns the wither of the property if available and if its first (only) parameter matches the type of the property.
	 *
	 * @return will never be {@literal null}.
	 */
	public Optional<Method> getWither() {
		return wither.get();
	}

	/**
	 * Returns the field of the property if available and if its first (only) parameter matches the type of the property.
	 *
	 * @return will never be {@literal null}.
	 */
	public Optional<Field> getField() {
		return this.field;
	}

	/**
	 * Returns whether the property exposes a getter or a setter.
	 *
	 * @return
	 */
	public boolean hasAccessor() {
		return getGetter().isPresent() || getSetter().isPresent();
	}

	/**
	 * Returns the name of the property.
	 *
	 * @return will never be {@literal null}.
	 */
	public String getName() {
		return this.name.get();
	}

	/**
	 * Returns the type of the property.
	 *
	 * @return will never be {@literal null}.
	 */
	public Class<?> getType() {
		return rawType;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(@Nullable Object obj) {

		if (this == obj) {
			return true;
		}

		if (!(obj instanceof Property)) {
			return false;
		}

		Property that = (Property) obj;
		if(this.typeInformation != null && that.typeInformation != null) {
			if(this.typeInformation != that.typeInformation) {
				return false;
			}
			if(!this.name.get().equals(that.name.get())) {
				return false;
			}
		}

		return this.field.isPresent() ? this.field.equals(that.field) : this.descriptor.equals(that.descriptor);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return hashCode.get();
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return toString.get();
	}

	/**
	 * Maps the backing {@link Field} or {@link PropertyDescriptor} using the given {@link Function}.
	 *
	 * @param function must not be {@literal null}.
	 * @return
	 */
	private <T> T withFieldOrDescriptor(Function<Object, T> function) {
		return withFieldOrDescriptor(function, function);
	}

	/**
	 * Maps the backing {@link Field} or {@link PropertyDescriptor} using the given functions.
	 *
	 * @param field must not be {@literal null}.
	 * @param descriptor must not be {@literal null}.
	 * @return
	 */
	private <T> T withFieldOrDescriptor(Function<? super Field, T> field,
			Function<? super PropertyDescriptor, T> descriptor) {

		return Optionals.firstNonEmpty(//
				() -> this.field.map(field), //
				() -> this.descriptor.map(descriptor))//
				.orElseThrow(() -> new IllegalStateException("Should not occur! Either field or descriptor has to be given"));
	}

	private static Optional<Method> findWither(TypeInformation<?> owner, String propertyName, Class<?> rawType) {

		AtomicReference<Method> resultHolder = new AtomicReference<>();
		String methodName = String.format("with%s", StringUtils.capitalize(propertyName));

		ReflectionUtils.doWithMethods(owner.getType(), it -> {

			if (owner.isAssignableFrom(owner.getReturnType(it))) {
				resultHolder.set(it);
			}
		}, it -> isMethodWithSingleParameterOfType(it, methodName, rawType));

		Method method = resultHolder.get();
		return method != null ? Optional.of(method) : Optional.empty();
	}

	private static boolean isMethodWithSingleParameterOfType(Method method, String name, Class<?> type) {

		return method.getParameterCount() == 1 //
				&& method.getName().equals(name) //
				&& method.getParameterTypes()[0].equals(type);
	}
}
