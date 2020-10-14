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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.springframework.lang.Nullable;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

/**
 * @author Christoph Strobl
 * @since 2020/10
 */
public class Field<T, O> implements AnnotationProvider {

	@Nullable Class<O> owner;
	String propertyName;

	TypeInformation<T> typeInformation;
	@Nullable TypeInformation<?> componentType;
	@Nullable TypeInformation<?> keyType;

	MultiValueMap<Class<? extends Annotation>, Annotation> annotations;

	@Nullable Function<O, T> getterFunction;
	@Nullable BiFunction<O, T, O> setterFunction;

	public Field(String propertyName, TypeInformation<T> propertyTypeInformation) {

		this.propertyName = propertyName;
		this.typeInformation = propertyTypeInformation;
		this.annotations = new LinkedMultiValueMap<>();
	}

	public static <T, O> Field<T, O> simple(Class<T> type, String propertyName) {

		if (type == String.class) {
			return (Field<T, O>) string(propertyName);
		}

		throw new IllegalArgumentException("Unknown simple type: " + type);
	}

	public static <S> Field<String, S> string(String propertyName) {
		return new Field<>(propertyName, StringTypeInformation.instance());
	}

	public static <S> Field<Long, S> int64(String propertyName) {
		return new Field<>(propertyName, StaticTypeInformation.from(Long.class));
	}

	public static <S> Field<Integer, S> int32(String propertyName) {
		return new Field<>(propertyName, StaticTypeInformation.from(Integer.class));
	}

	public static <S, T> Field<T, S> type(String propertyName, TypeInformation<T> type) {
		return new Field<>(propertyName, type);
	}

	public Field<T, O> annotation(Annotation annotation) {

		annotations.add(annotation.annotationType(), annotation);
		return this;
	}

	public Field<T, O> wither(BiFunction<O, T, O> setterFunction) {

		this.setterFunction = setterFunction;
		return this;
	}

	public Field<T, O> setter(BiConsumer<O, T> setterFunction) {

		return wither((o, t) -> {

			setterFunction.accept(o, t);
			return o;
		});
	}

	public Field<T, O> getter(Function<O, T> getterFunction) {

		this.getterFunction = getterFunction;
		return this;
	}

	public Field<T, O> valueType(TypeInformation<?> valueTypeInformation) {
		this.componentType = valueTypeInformation;
		return this;
	}

	Field<T, O> owner(Class<O> owner) {

		this.owner = owner;
		return this;
	}

	public TypeInformation<?> getValueType() {
		return componentType != null ? componentType : typeInformation;
	}

	public String getFieldName() {
		return propertyName;
	}

	public TypeInformation<T> getTypeInformation() {
		return typeInformation;
	}

	public boolean hasSetter() {
		return setterFunction != null;
	}

	public boolean hasGetter() {
		return getterFunction != null;
	}

	public BiFunction<O, T, O> getSetter() {
		return setterFunction;
	}

	@Nullable
	public Function<O, T> getGetter() {
		return getterFunction;
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
}
