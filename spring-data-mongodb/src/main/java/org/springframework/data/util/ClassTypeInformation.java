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
package org.springframework.data.util;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.springframework.core.GenericTypeResolver;
import org.springframework.util.Assert;
import org.springframework.util.ConcurrentReferenceHashMap;
import org.springframework.util.ConcurrentReferenceHashMap.ReferenceType;

/**
 * {@link TypeInformation} for a plain {@link Class}.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class ClassTypeInformation<S> extends TypeDiscoverer<S> {

	public static final ClassTypeInformation<Collection> COLLECTION = new ClassTypeInformation(Collection.class);
	public static final ClassTypeInformation<List> LIST = new ClassTypeInformation(List.class);
	public static final ClassTypeInformation<Set> SET = new ClassTypeInformation(Set.class);
	public static final ClassTypeInformation<Map> MAP = new ClassTypeInformation(Map.class);
	public static final ClassTypeInformation<Object> OBJECT = new ClassTypeInformation(Object.class);

	private static final Map<Class<?>, ClassTypeInformation<?>> cache = new ConcurrentReferenceHashMap<>(64,
			ReferenceType.WEAK);

	static {
		Arrays.asList(COLLECTION, LIST, SET, MAP, OBJECT).forEach(it -> cache.put(it.getType(), it));
	}

	private final Class<S> type;

	public static void warmCache(ClassTypeInformation<?>... typeInformations) {
		for(ClassTypeInformation<?> information : typeInformations) {
			cache.put(information.getType(), information);
		}
	}

	/**
	 * Simple factory method to easily create new instances of {@link ClassTypeInformation}.
	 *
	 * @param <S>
	 * @param type must not be {@literal null}.
	 * @return
	 */
	public static <S> ClassTypeInformation<S> from(Class<S> type) {

		Assert.notNull(type, "Type must not be null!");

		return (ClassTypeInformation<S>) cache.computeIfAbsent(type, ClassTypeInformation::new);
	}

	/**
	 * Creates a {@link TypeInformation} from the given method's return type.
	 *
	 * @param method must not be {@literal null}.
	 * @return
	 */
	public static <S> TypeInformation<S> fromReturnTypeOf(Method method) {

		Assert.notNull(method, "Method must not be null!");
		return (TypeInformation<S>) ClassTypeInformation.from(method.getDeclaringClass())
				.createInfo(method.getGenericReturnType());
	}

	/**
	 * Creates {@link ClassTypeInformation} for the given type.
	 *
	 * @param type
	 */
	ClassTypeInformation(Class<S> type) {
		super(ProxyUtils.getUserClass(type), getTypeVariableMap(type));
		this.type = type;
	}

	ClassTypeInformation(Class<S> type, TypeInformation<?> componentType, TypeInformation<?> keyType) {
		super(type, componentType, keyType);
		this.type = type;
	}

	/**
	 * Little helper to allow us to create a generified map, actually just to satisfy the compiler.
	 *
	 * @param type must not be {@literal null}.
	 * @return
	 */
	private static Map<TypeVariable<?>, Type> getTypeVariableMap(Class<?> type) {
		return getTypeVariableMap(type, new HashSet<>());
	}

	private static Map<TypeVariable<?>, Type> getTypeVariableMap(Class<?> type, Collection<Type> visited) {

		if (visited.contains(type)) {
			return Collections.emptyMap();
		} else {
			visited.add(type);
		}

		Map<TypeVariable, Type> source = GenericTypeResolver.getTypeVariableMap(type);
		Map<TypeVariable<?>, Type> map = new HashMap<>(source.size());

		for (Entry<TypeVariable, Type> entry : source.entrySet()) {

			Type value = entry.getValue();
			map.put(entry.getKey(), entry.getValue());

			if (value instanceof Class) {

				for (Entry<TypeVariable<?>, Type> nestedEntry : getTypeVariableMap((Class<?>) value, visited).entrySet()) {
					if (!map.containsKey(nestedEntry.getKey())) {
						map.put(nestedEntry.getKey(), nestedEntry.getValue());
					}
				}
			}
		}

		return map;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.util.TypeDiscoverer#getType()
	 */
	@Override
	public Class<S> getType() {
		return type;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.util.TypeDiscoverer#getRawTypeInformation()
	 */
	@Override
	public ClassTypeInformation<?> getRawTypeInformation() {
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.util.TypeDiscoverer#isAssignableFrom(org.springframework.data.util.TypeInformation)
	 */
	@Override
	public boolean isAssignableFrom(TypeInformation<?> target) {
		return getType().isAssignableFrom(target.getType());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.util.TypeDiscoverer#specialize(org.springframework.data.util.ClassTypeInformation)
	 */
	@Override
	public TypeInformation<? extends S> specialize(ClassTypeInformation<?> type) {
		return (TypeInformation<? extends S>) type;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return type.getName();
	}
}
