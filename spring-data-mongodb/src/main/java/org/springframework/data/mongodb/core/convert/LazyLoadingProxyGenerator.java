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
package org.springframework.data.mongodb.core.convert;

import static org.springframework.util.ReflectionUtils.*;

import java.io.Serializable;
import java.lang.reflect.Method;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.cglib.proxy.Callback;
import org.springframework.cglib.proxy.Enhancer;
import org.springframework.cglib.proxy.Factory;
import org.springframework.cglib.proxy.MethodProxy;
import org.springframework.data.mongodb.core.convert.ReferenceResolver.LookupFunction;
import org.springframework.data.mongodb.core.convert.ReferenceResolver.ResultConversionFunction;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.objenesis.ObjenesisStd;
import org.springframework.util.ReflectionUtils;

/**
 * @author Christoph Strobl
 */
class LazyLoadingProxyGenerator {

	private final ObjenesisStd objenesis;
	private final ReferenceReader referenceReader;

	public LazyLoadingProxyGenerator(ReferenceReader referenceReader) {

		this.referenceReader = referenceReader;
		this.objenesis = new ObjenesisStd(true);
	}

	public Object createLazyLoadingProxy(MongoPersistentProperty property, Object source, LookupFunction lookupFunction,
			ResultConversionFunction resultConversionFunction) {

		Class<?> propertyType = property.getType();
		LazyLoadingInterceptor interceptor = new LazyLoadingInterceptor(property, source, referenceReader, lookupFunction,
				resultConversionFunction);

		if (!propertyType.isInterface()) {

			Factory factory = (Factory) objenesis.newInstance(getEnhancedTypeFor(propertyType));
			factory.setCallbacks(new Callback[] { interceptor });

			return factory;
		}

		ProxyFactory proxyFactory = new ProxyFactory();

		for (Class<?> type : propertyType.getInterfaces()) {
			proxyFactory.addInterface(type);
		}

		proxyFactory.addInterface(LazyLoadingProxy.class);
		proxyFactory.addInterface(propertyType);
		proxyFactory.addAdvice(interceptor);

		return proxyFactory.getProxy(LazyLoadingProxy.class.getClassLoader());
	}

	/**
	 * Returns the CGLib enhanced type for the given source type.
	 *
	 * @param type
	 * @return
	 */
	private Class<?> getEnhancedTypeFor(Class<?> type) {

		Enhancer enhancer = new Enhancer();
		enhancer.setSuperclass(type);
		enhancer.setCallbackType(org.springframework.cglib.proxy.MethodInterceptor.class);
		enhancer.setInterfaces(new Class[] { LazyLoadingProxy.class });

		return enhancer.createClass();
	}

	public static class LazyLoadingInterceptor
			implements MethodInterceptor, org.springframework.cglib.proxy.MethodInterceptor, Serializable {

		private final ReferenceReader referenceReader;
		MongoPersistentProperty property;
		private volatile boolean resolved;
		private @org.springframework.lang.Nullable Object result;
		private Object source;
		private LookupFunction lookupFunction;
		private ResultConversionFunction resultConversionFunction;

		private final Method INITIALIZE_METHOD, TO_DBREF_METHOD, FINALIZE_METHOD, GET_SOURCE_METHOD;

		{
			try {
				INITIALIZE_METHOD = LazyLoadingProxy.class.getMethod("getTarget");
				TO_DBREF_METHOD = LazyLoadingProxy.class.getMethod("toDBRef");
				FINALIZE_METHOD = Object.class.getDeclaredMethod("finalize");
				GET_SOURCE_METHOD = LazyLoadingProxy.class.getMethod("getSource");
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		public LazyLoadingInterceptor(MongoPersistentProperty property, Object source, ReferenceReader reader,
				LookupFunction lookupFunction, ResultConversionFunction resultConversionFunction) {

			this.property = property;
			this.source = source;
			this.referenceReader = reader;
			this.lookupFunction = lookupFunction;
			this.resultConversionFunction = resultConversionFunction;
		}

		@Nullable
		@Override
		public Object invoke(@Nonnull MethodInvocation invocation) throws Throwable {
			return intercept(invocation.getThis(), invocation.getMethod(), invocation.getArguments(), null);
		}

		@Override
		public Object intercept(Object o, Method method, Object[] args, MethodProxy proxy) throws Throwable {

			if (INITIALIZE_METHOD.equals(method)) {
				return ensureResolved();
			}

			if (TO_DBREF_METHOD.equals(method)) {
				return null;
			}

			if (GET_SOURCE_METHOD.equals(method)) {
				return source;
			}

			if (isObjectMethod(method) && Object.class.equals(method.getDeclaringClass())) {

				if (ReflectionUtils.isToStringMethod(method)) {
					return proxyToString(proxy);
				}

				if (ReflectionUtils.isEqualsMethod(method)) {
					return proxyEquals(proxy, args[0]);
				}

				if (ReflectionUtils.isHashCodeMethod(method)) {
					return proxyHashCode(proxy);
				}

				// DATAMONGO-1076 - finalize methods should not trigger proxy initialization
				if (FINALIZE_METHOD.equals(method)) {
					return null;
				}
			}

			Object target = ensureResolved();

			if (target == null) {
				return null;
			}

			ReflectionUtils.makeAccessible(method);

			return method.invoke(target, args);
		}

		private Object ensureResolved() {

			if (!resolved) {
				this.result = resolve();
				this.resolved = true;
			}

			return this.result;
		}

		private String proxyToString(Object source) {

			StringBuilder description = new StringBuilder();
			if (source != null) {
				description.append(source);
			} else {
				description.append(System.identityHashCode(source));
			}
			description.append("$").append(LazyLoadingProxy.class.getSimpleName());

			return description.toString();
		}

		private boolean proxyEquals(@org.springframework.lang.Nullable Object proxy, Object that) {

			if (!(that instanceof LazyLoadingProxy)) {
				return false;
			}

			if (that == proxy) {
				return true;
			}

			return proxyToString(proxy).equals(that.toString());
		}

		private int proxyHashCode(@org.springframework.lang.Nullable Object proxy) {
			return proxyToString(proxy).hashCode();
		}

		@org.springframework.lang.Nullable
		private synchronized Object resolve() {

			if (resolved) {

				// if (LOGGER.isTraceEnabled()) {
				// LOGGER.trace("Accessing already resolved lazy loading property {}.{}",
				// property.getOwner() != null ? property.getOwner().getName() : "unknown", property.getName());
				// }
				return result;
			}

			try {
				// if (LOGGER.isTraceEnabled()) {
				// LOGGER.trace("Resolving lazy loading property {}.{}",
				// property.getOwner() != null ? property.getOwner().getName() : "unknown", property.getName());
				// }

				return referenceReader.readReference(property, source, lookupFunction, resultConversionFunction);

			} catch (RuntimeException ex) {
				throw ex;

				// DataAccessException translatedException = this.exceptionTranslator.translateExceptionIfPossible(ex);
				//
				// if (translatedException instanceof ClientSessionException) {
				// throw new LazyLoadingException("Unable to lazily resolve DBRef! Invalid session state.", ex);
				// }

				// throw new LazyLoadingException("Unable to lazily resolve DBRef!",
				// translatedException != null ? translatedException : ex);
			}
		}
	}
}
