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
package org.springframework.data.mongodb.core;

import java.lang.reflect.Method;
import java.util.function.Supplier;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.springframework.lang.Nullable;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.ReflectionUtils;

import com.mongodb.session.ClientSession;

/**
 * {@link MethodInterceptor} implementation looking up and invoking an alternative target method having
 * {@link ClientSession} as its first argument. This allows seamless integration with the existing code base.
 * 
 * @since 2.1
 */
class SessionAwareMethodInterceptor implements MethodInterceptor {

	private final Supplier<ClientSession> session;
	private final Object target;
	private final Class<?> targetType;

	<T> SessionAwareMethodInterceptor(ClientSession session, T target, @Nullable Class<? super T> targetType) {

		this.session = () -> session;
		this.target = target;
		this.targetType = ClassUtils.getUserClass(targetType == null ? target.getClass() : targetType);
	}

	@Override
	public Object invoke(MethodInvocation methodInvocation) throws Throwable {

		if (!requiresSession(methodInvocation)) {
			return methodInvocation.proceed();
		}

		Method targetMethod = findTargetWithSession(methodInvocation.getMethod());
		return targetMethod == null ? methodInvocation.proceed()
				: ReflectionUtils.invokeMethod(targetMethod, target, prependSessionToArguments(methodInvocation));
	}

	private boolean requiresSession(MethodInvocation methodInvocation) {

		if (ObjectUtils.isEmpty(methodInvocation.getMethod().getParameterTypes())
				|| !ClassUtils.isAssignable(ClientSession.class, methodInvocation.getMethod().getParameterTypes()[0])) {
			return true;
		}

		return false;
	}

	private Method findTargetWithSession(Method sourceMethod) {

		// TODO: should we be smart and pre identify methods so we do not have to look them up each time? - Think about it

		Class<?>[] argTypes = sourceMethod.getParameterTypes();
		Class<?>[] args = new Class<?>[argTypes.length + 1];
		args[0] = ClientSession.class;
		System.arraycopy(argTypes, 0, args, 1, argTypes.length);

		return ReflectionUtils.findMethod(targetType, sourceMethod.getName(), args);
	}

	private Object[] prependSessionToArguments(MethodInvocation invocation) {

		Object[] args = new Object[invocation.getArguments().length + 1];
		args[0] = session.get();
		System.arraycopy(invocation.getArguments(), 0, args, 1, invocation.getArguments().length);
		return args;
	}
}
