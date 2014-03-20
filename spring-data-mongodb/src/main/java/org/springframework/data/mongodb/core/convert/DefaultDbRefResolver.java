/*
 * Copyright 2013-2014 the original author or authors.
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
package org.springframework.data.mongodb.core.convert;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Method;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.objenesis.Objenesis;
import org.objenesis.ObjenesisStd;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.cglib.proxy.Callback;
import org.springframework.cglib.proxy.Enhancer;
import org.springframework.cglib.proxy.Factory;
import org.springframework.cglib.proxy.MethodProxy;
import org.springframework.core.SpringVersion;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.mongodb.LazyLoadingException;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

import com.mongodb.DB;
import com.mongodb.DBRef;

/**
 * A {@link DbRefResolver} that resolves {@link org.springframework.data.mongodb.core.mapping.DBRef}s by delegating to a
 * {@link DbRefResolverCallback} than is able to generate lazy loading proxies.
 * 
 * @author Thomas Darimont
 * @author Oliver Gierke
 */
public class DefaultDbRefResolver implements DbRefResolver {

	private static final boolean IS_SPRING_4_OR_BETTER = SpringVersion.getVersion().startsWith("4");
	private static final boolean OBJENESIS_PRESENT = ClassUtils.isPresent("org.objenesis.Objenesis", null);

	private final MongoDbFactory mongoDbFactory;
	private final PersistenceExceptionTranslator exceptionTranslator;

	/**
	 * Creates a new {@link DefaultDbRefResolver} with the given {@link MongoDbFactory}.
	 * 
	 * @param mongoDbFactory must not be {@literal null}.
	 */
	public DefaultDbRefResolver(MongoDbFactory mongoDbFactory) {

		Assert.notNull(mongoDbFactory, "MongoDbFactory translator must not be null!");

		this.mongoDbFactory = mongoDbFactory;
		this.exceptionTranslator = mongoDbFactory.getExceptionTranslator();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.convert.DbRefResolver#resolveDbRef(org.springframework.data.mongodb.core.mapping.MongoPersistentProperty, org.springframework.data.mongodb.core.convert.DbRefResolverCallback)
	 */
	@Override
	public Object resolveDbRef(MongoPersistentProperty property, DbRefResolverCallback callback) {

		Assert.notNull(property, "Property must not be null!");
		Assert.notNull(callback, "Callback must not be null!");

		if (isLazyDbRef(property)) {
			return createLazyLoadingProxy(property, callback);
		}

		return callback.resolve(property);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.convert.DbRefResolver#created(org.springframework.data.mongodb.core.mapping.MongoPersistentProperty, org.springframework.data.mongodb.core.mapping.MongoPersistentEntity, java.lang.Object)
	 */
	@Override
	public DBRef createDbRef(org.springframework.data.mongodb.core.mapping.DBRef annotation,
			MongoPersistentEntity<?> entity, Object id) {

		DB db = mongoDbFactory.getDb();
		db = annotation != null && StringUtils.hasText(annotation.db()) ? mongoDbFactory.getDb(annotation.db()) : db;

		return new DBRef(db, entity.getCollection(), id);
	}

	/**
	 * Creates a proxy for the given {@link MongoPersistentProperty} using the given {@link DbRefResolverCallback} to
	 * eventually resolve the value of the property.
	 * 
	 * @param property must not be {@literal null}.
	 * @param callback must not be {@literal null}.
	 * @return
	 */
	private Object createLazyLoadingProxy(MongoPersistentProperty property, DbRefResolverCallback callback) {

		ProxyFactory proxyFactory = new ProxyFactory();
		Class<?> propertyType = property.getType();

		for (Class<?> type : propertyType.getInterfaces()) {
			proxyFactory.addInterface(type);
		}

		LazyLoadingInterceptor interceptor = new LazyLoadingInterceptor(property, exceptionTranslator, callback);

		if (propertyType.isInterface()) {
			proxyFactory.addInterface(propertyType);
			proxyFactory.addAdvice(interceptor);
			return proxyFactory.getProxy();
		}

		proxyFactory.setProxyTargetClass(true);
		proxyFactory.setTargetClass(propertyType);

		if (IS_SPRING_4_OR_BETTER || !OBJENESIS_PRESENT) {
			proxyFactory.addAdvice(interceptor);
			return proxyFactory.getProxy();
		}

		return ObjenesisProxyEnhancer.enhanceAndGet(proxyFactory, propertyType, interceptor);
	}

	/**
	 * @param property
	 * @return
	 */
	private boolean isLazyDbRef(MongoPersistentProperty property) {
		return property.getDBRef() != null && property.getDBRef().lazy();
	}

	/**
	 * A {@link MethodInterceptor} that is used within a lazy loading proxy. The property resolving is delegated to a
	 * {@link DbRefResolverCallback}. The resolving process is triggered by a method invocation on the proxy and is
	 * guaranteed to be performed only once.
	 * 
	 * @author Thomas Darimont
	 */
	static class LazyLoadingInterceptor implements MethodInterceptor, org.springframework.cglib.proxy.MethodInterceptor,
			Serializable {

		private final DbRefResolverCallback callback;
		private final MongoPersistentProperty property;
		private final PersistenceExceptionTranslator exceptionTranslator;

		private volatile boolean resolved;
		private Object result;

		/**
		 * Creates a new {@link LazyLoadingInterceptor} for the given {@link MongoPersistentProperty},
		 * {@link PersistenceExceptionTranslator} and {@link DbRefResolverCallback}.
		 * 
		 * @param property must not be {@literal null}.
		 * @param callback must not be {@literal null}.
		 */
		public LazyLoadingInterceptor(MongoPersistentProperty property, PersistenceExceptionTranslator exceptionTranslator,
				DbRefResolverCallback callback) {

			Assert.notNull(property, "Property must not be null!");
			Assert.notNull(exceptionTranslator, "Exception translator must not be null!");
			Assert.notNull(callback, "Callback must not be null!");

			this.callback = callback;
			this.exceptionTranslator = exceptionTranslator;
			this.property = property;
		}

		/*
		 * (non-Javadoc)
		 * @see org.aopalliance.intercept.MethodInterceptor#invoke(org.aopalliance.intercept.MethodInvocation)
		 */
		@Override
		public Object invoke(MethodInvocation invocation) throws Throwable {
			return intercept(invocation.getThis(), invocation.getMethod(), invocation.getArguments(), null);
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.cglib.proxy.MethodInterceptor#intercept(java.lang.Object, java.lang.reflect.Method, java.lang.Object[], org.springframework.cglib.proxy.MethodProxy)
		 */
		@Override
		public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
			return obj!=null && ReflectionUtils.isObjectMethod(method) && method.getDeclaringClass() == Object.class ? method.invoke(obj,
					args) : method.invoke(ensureResolved(), args);
		}

		private Object ensureResolved() {

			if (!resolved) {
				this.result = resolve();
				this.resolved = true;
			}

			return this.result;
		}

		private void writeObject(ObjectOutputStream out) throws IOException {

			ensureResolved();
			out.writeObject(this.result);
		}

		private void readObject(ObjectInputStream in) throws IOException {

			try {
				this.resolved = true; // Object is guaranteed to be resolved after serializations
				this.result = in.readObject();
			} catch (ClassNotFoundException e) {
				throw new LazyLoadingException("Could not deserialize result", e);
			}
		}

		/**
		 * @return
		 */
		private synchronized Object resolve() {

			if (!resolved) {

				try {

					return callback.resolve(property);

				} catch (RuntimeException ex) {

					DataAccessException translatedException = this.exceptionTranslator.translateExceptionIfPossible(ex);
					throw new LazyLoadingException("Unable to lazily resolve DBRef!", translatedException);
				}
			}

			return result;
		}

		public boolean isResolved() {
			return resolved;
		}

		public Object getResult() {
			return result;
		}
	}

	/**
	 * Static class to accomodate optional dependency on Objenesis.
	 * 
	 * @author Oliver Gierke
	 */
	private static class ObjenesisProxyEnhancer {

		private static final Objenesis OBJENESIS = new ObjenesisStd(true);

		public static Object enhanceAndGet(ProxyFactory proxyFactory, Class<?> type,
				org.springframework.cglib.proxy.MethodInterceptor interceptor) {

			Enhancer enhancer = new Enhancer();
			enhancer.setSuperclass(type);
			enhancer.setCallbackType(org.springframework.cglib.proxy.MethodInterceptor.class);

			Factory factory = (Factory) OBJENESIS.newInstance(enhancer.createClass());
			factory.setCallbacks(new Callback[] { interceptor });
			return factory;
		}
	}
}
