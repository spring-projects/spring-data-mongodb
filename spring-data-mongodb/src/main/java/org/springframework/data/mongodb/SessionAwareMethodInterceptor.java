/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.mongodb;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Optional;
import java.util.function.BiFunction;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.springframework.core.MethodClassKey;
import org.springframework.lang.Nullable;
import org.springframework.util.ClassUtils;
import org.springframework.util.ConcurrentReferenceHashMap;
import org.springframework.util.ObjectUtils;
import org.springframework.util.ReflectionUtils;

import com.mongodb.WriteConcern;
import com.mongodb.session.ClientSession;

/**
 * {@link MethodInterceptor} implementation looking up and invoking an alternative target method having
 * {@link ClientSession} as its first argument. This allows seamless integration with the existing code base.
 * <p />
 * The {@link MethodInterceptor} is aware of methods on {@code MongoCollection} that my return new instances of itself
 * like (eg. {@link com.mongodb.reactivestreams.client.MongoCollection#withWriteConcern(WriteConcern)} and decorate them
 * if not already proxied.
 *
 * @since 2.1
 */
public class SessionAwareMethodInterceptor<D, C> implements MethodInterceptor {

	private static final MethodCache METHOD_CACHE = new MethodCache();

	private final ClientSession session;
	private final BiFunction collectionDecorator;
	private final BiFunction databaseDecorator;
	private final Object target;
	private final Class<?> targetType;
	private final Class<?> collectionType;
	private final Class<?> databaseType;

	/**
	 * Create a new SessionAwareMethodInterceptor for given target.
	 * 
	 * @param session the {@link ClientSession} to be used on invocation.
	 * @param target the original target object.
	 * @param databaseType the MongoDB database type
	 * @param databaseDecorator a {@link BiFunction} used to create the proxy for an imperative / reactive
	 *          {@code MongoDatabase}.
	 * @param collectionType the MongoDB collection type.
	 * @param collectionCallback a {@link BiFunction} used to create the proxy for an imperative / reactive
	 *          {@code MongoCollection}.
	 * @param <T>
	 */
	public <T> SessionAwareMethodInterceptor(ClientSession session, T target, Class<D> databaseType,
			BiFunction<ClientSession, D, D> databaseDecorator, Class<C> collectionType,
			BiFunction<ClientSession, C, C> collectionDecorator) {

		this.session = session;
		this.target = target;
		this.databaseType = ClassUtils.getUserClass(databaseType);
		this.collectionType = ClassUtils.getUserClass(collectionType);
		this.collectionDecorator = collectionDecorator;
		this.databaseDecorator = databaseDecorator;

		this.targetType = ClassUtils.isAssignable(databaseType, target.getClass()) ? databaseType : collectionType;
	}

	/*
	 * (non-Javadoc)
	 * @see org.aopalliance.intercept.MethodInterceptor(org.aopalliance.intercept.MethodInvocation)
	 */
	@Override
	public Object invoke(MethodInvocation methodInvocation) throws Throwable {

		if (requiresDecoration(methodInvocation)) {

			Object target = methodInvocation.proceed();
			if (target instanceof Proxy) {
				return target;
			}

			return decorate(target);
		}

		if (!requiresSession(methodInvocation)) {
			return methodInvocation.proceed();
		}

		Optional<Method> targetMethod = METHOD_CACHE.lookup(methodInvocation.getMethod(), targetType);

		return !targetMethod.isPresent() ? methodInvocation.proceed()
				: ReflectionUtils.invokeMethod(targetMethod.get(), target, prependSessionToArguments(methodInvocation));
	}

	private boolean requiresDecoration(MethodInvocation methodInvocation) {

		return ClassUtils.isAssignable(databaseType, methodInvocation.getMethod().getReturnType())
				|| ClassUtils.isAssignable(collectionType, methodInvocation.getMethod().getReturnType());
	}

	protected Object decorate(Object target) {

		return ClassUtils.isAssignable(databaseType, target.getClass()) ? databaseDecorator.apply(session, target)
				: collectionDecorator.apply(session, target);
	}

	private boolean requiresSession(MethodInvocation methodInvocation) {

		if (ObjectUtils.isEmpty(methodInvocation.getMethod().getParameterTypes())
				|| !ClassUtils.isAssignable(ClientSession.class, methodInvocation.getMethod().getParameterTypes()[0])) {
			return true;
		}

		return false;
	}

	private Object[] prependSessionToArguments(MethodInvocation invocation) {

		Object[] args = new Object[invocation.getArguments().length + 1];
		args[0] = session;
		System.arraycopy(invocation.getArguments(), 0, args, 1, invocation.getArguments().length);
		return args;
	}

	/**
	 * Simple {@link Method} to {@link Method} caching facility for {@link ClientSession} overloaded targets.
	 *
	 * @since 2.1
	 * @author Christoph Strobl
	 */
	static class MethodCache {

		private final ConcurrentReferenceHashMap<MethodClassKey, Optional<Method>> cache = new ConcurrentReferenceHashMap<>();

		Optional<Method> lookup(Method method, Class<?> targetClass) {

			return cache.computeIfAbsent(new MethodClassKey(method, targetClass),
					val -> Optional.ofNullable(findTargetWithSession(method, targetClass)));
		}

		@Nullable
		private Method findTargetWithSession(Method sourceMethod, Class<?> targetType) {

			Class<?>[] argTypes = sourceMethod.getParameterTypes();
			Class<?>[] args = new Class<?>[argTypes.length + 1];
			args[0] = ClientSession.class;
			System.arraycopy(argTypes, 0, args, 1, argTypes.length);

			return ReflectionUtils.findMethod(targetType, sourceMethod.getName(), args);
		}

		boolean contains(Method method, Class<?> targetClass) {
			return cache.containsKey(new MethodClassKey(method, targetClass));
		}
	}

}
