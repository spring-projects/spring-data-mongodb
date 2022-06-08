/*
 * Copyright 2021-2022 the original author or authors.
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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Method;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.aop.framework.ProxyFactory;
import org.springframework.cglib.proxy.Callback;
import org.springframework.cglib.proxy.Enhancer;
import org.springframework.cglib.proxy.Factory;
import org.springframework.cglib.proxy.MethodProxy;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.mongodb.ClientSessionException;
import org.springframework.data.mongodb.LazyLoadingException;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.lang.Nullable;
import org.springframework.objenesis.ObjenesisStd;
import org.springframework.util.ReflectionUtils;

import com.mongodb.DBRef;

/**
 * {@link ProxyFactory} to create a proxy for {@link MongoPersistentProperty#getType()} to resolve a reference lazily.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
class LazyLoadingProxyFactory {

	private static final Log LOGGER = LogFactory.getLog(LazyLoadingProxyFactory.class);

	private final ObjenesisStd objenesis;

	private final PersistenceExceptionTranslator exceptionTranslator;

	public LazyLoadingProxyFactory(PersistenceExceptionTranslator exceptionTranslator) {
		this.exceptionTranslator = exceptionTranslator;
		this.objenesis = new ObjenesisStd(true);
	}

	public Object createLazyLoadingProxy(MongoPersistentProperty property, DbRefResolverCallback callback,
			Object source) {

		Class<?> propertyType = property.getType();
		LazyLoadingInterceptor interceptor = new LazyLoadingInterceptor(property, callback, source, exceptionTranslator);

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

		private static final Method INITIALIZE_METHOD, TO_DBREF_METHOD, FINALIZE_METHOD, GET_SOURCE_METHOD;

		static {
			try {
				INITIALIZE_METHOD = LazyLoadingProxy.class.getMethod("getTarget");
				TO_DBREF_METHOD = LazyLoadingProxy.class.getMethod("toDBRef");
				FINALIZE_METHOD = Object.class.getDeclaredMethod("finalize");
				GET_SOURCE_METHOD = LazyLoadingProxy.class.getMethod("getSource");
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		private final MongoPersistentProperty property;
		private final DbRefResolverCallback callback;
		private final Object source;
		private final PersistenceExceptionTranslator exceptionTranslator;
		private volatile boolean resolved;
		private @Nullable Object result;

		public LazyLoadingInterceptor(MongoPersistentProperty property, DbRefResolverCallback callback, Object source,
				PersistenceExceptionTranslator exceptionTranslator) {

			this.property = property;
			this.callback = callback;
			this.source = source;
			this.exceptionTranslator = exceptionTranslator;
		}

		@Nullable
		@Override
		public Object invoke(MethodInvocation invocation) throws Throwable {
			return intercept(invocation.getThis(), invocation.getMethod(), invocation.getArguments(), null);
		}

		@Nullable
		@Override
		public Object intercept(Object o, Method method, Object[] args, MethodProxy proxy) throws Throwable {

			if (INITIALIZE_METHOD.equals(method)) {
				return ensureResolved();
			}

			if (TO_DBREF_METHOD.equals(method)) {
				return source instanceof DBRef ? source : null;
			}

			if (GET_SOURCE_METHOD.equals(method)) {
				return source;
			}

			if (isObjectMethod(method) && Object.class.equals(method.getDeclaringClass())) {

				if (ReflectionUtils.isToStringMethod(method)) {
					return proxyToString(source);
				}

				if (ReflectionUtils.isEqualsMethod(method)) {
					return proxyEquals(o, args[0]);
				}

				if (ReflectionUtils.isHashCodeMethod(method)) {
					return proxyHashCode();
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

		@Nullable
		private Object ensureResolved() {

			if (!resolved) {
				this.result = resolve();
				this.resolved = true;
			}

			return this.result;
		}

		private String proxyToString(@Nullable Object source) {

			StringBuilder description = new StringBuilder();
			if (source != null) {
				if (source instanceof DBRef) {
					description.append(((DBRef) source).getCollectionName());
					description.append(":");
					description.append(((DBRef) source).getId());
				} else {
					description.append(source);
				}
			} else {
				description.append(System.identityHashCode(source));
			}
			description.append("$").append(LazyLoadingProxy.class.getSimpleName());

			return description.toString();
		}

		private boolean proxyEquals(@Nullable Object proxy, Object that) {

			if (!(that instanceof LazyLoadingProxy)) {
				return false;
			}

			if (that == proxy) {
				return true;
			}

			return proxyToString(proxy).equals(that.toString());
		}

		private int proxyHashCode() {
			return proxyToString(source).hashCode();
		}

		/**
		 * Callback method for serialization.
		 *
		 * @param out
		 * @throws IOException
		 */
		private void writeObject(ObjectOutputStream out) throws IOException {

			ensureResolved();
			out.writeObject(this.result);
		}

		/**
		 * Callback method for deserialization.
		 *
		 * @param in
		 * @throws IOException
		 */
		private void readObject(ObjectInputStream in) throws IOException {

			try {
				this.resolved = true;
				this.result = in.readObject();
			} catch (ClassNotFoundException e) {
				throw new LazyLoadingException("Could not deserialize result", e);
			}
		}

		@Nullable
		private synchronized Object resolve() {

			if (resolved) {

				if (LOGGER.isTraceEnabled()) {
					LOGGER.trace(String.format("Accessing already resolved lazy loading property %s.%s",
							property.getOwner() != null ? property.getOwner().getName() : "unknown", property.getName()));
				}
				return result;
			}

			try {
				if (LOGGER.isTraceEnabled()) {
					LOGGER.trace(String.format("Resolving lazy loading property %s.%s",
							property.getOwner() != null ? property.getOwner().getName() : "unknown", property.getName()));
				}

				return callback.resolve(property);

			} catch (RuntimeException ex) {

				DataAccessException translatedException = exceptionTranslator.translateExceptionIfPossible(ex);

				if (translatedException instanceof ClientSessionException) {
					throw new LazyLoadingException("Unable to lazily resolve DBRef; Invalid session state", ex);
				}

				throw new LazyLoadingException("Unable to lazily resolve DBRef",
						translatedException != null ? translatedException : ex);
			}
		}
	}

}
