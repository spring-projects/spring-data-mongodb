/*
 * Copyright 2013-2019 the original author or authors.
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.cglib.proxy.Callback;
import org.springframework.cglib.proxy.Enhancer;
import org.springframework.cglib.proxy.Factory;
import org.springframework.cglib.proxy.MethodProxy;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.mongodb.ClientSessionException;
import org.springframework.data.mongodb.LazyLoadingException;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.lang.Nullable;
import org.springframework.objenesis.ObjenesisStd;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

import com.mongodb.DBRef;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;

/**
 * A {@link DbRefResolver} that resolves {@link org.springframework.data.mongodb.core.mapping.DBRef}s by delegating to a
 * {@link DbRefResolverCallback} than is able to generate lazy loading proxies.
 *
 * @author Thomas Darimont
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.4
 */
public class DefaultDbRefResolver implements DbRefResolver {

	private static final Logger LOGGER = LoggerFactory.getLogger(DefaultDbRefResolver.class);

	private final MongoDbFactory mongoDbFactory;
	private final PersistenceExceptionTranslator exceptionTranslator;
	private final ObjenesisStd objenesis;

	/**
	 * Creates a new {@link DefaultDbRefResolver} with the given {@link MongoDbFactory}.
	 *
	 * @param mongoDbFactory must not be {@literal null}.
	 */
	public DefaultDbRefResolver(MongoDbFactory mongoDbFactory) {

		Assert.notNull(mongoDbFactory, "MongoDbFactory translator must not be null!");

		this.mongoDbFactory = mongoDbFactory;
		this.exceptionTranslator = mongoDbFactory.getExceptionTranslator();
		this.objenesis = new ObjenesisStd(true);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.convert.DbRefResolver#resolveDbRef(org.springframework.data.mongodb.core.mapping.MongoPersistentProperty, org.springframework.data.mongodb.core.convert.DbRefResolverCallback)
	 */
	@Override
	public Object resolveDbRef(MongoPersistentProperty property, @Nullable DBRef dbref, DbRefResolverCallback callback,
			DbRefProxyHandler handler) {

		Assert.notNull(property, "Property must not be null!");
		Assert.notNull(callback, "Callback must not be null!");
		Assert.notNull(handler, "Handler must not be null!");

		if (isLazyDbRef(property)) {
			return createLazyLoadingProxy(property, dbref, callback, handler);
		}

		return callback.resolve(property);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.convert.DbRefResolver#fetch(com.mongodb.DBRef)
	 */
	@Override
	public Document fetch(DBRef dbRef) {

		if (LOGGER.isTraceEnabled()) {
			LOGGER.trace("Fetching DBRef '{}' from {}.{}.", dbRef.getId(),
					StringUtils.hasText(dbRef.getDatabaseName()) ? dbRef.getDatabaseName() : mongoDbFactory.getMongoDatabase().getName(),
					dbRef.getCollectionName());
		}

		StringUtils.hasText(dbRef.getDatabaseName());
		return getCollection(dbRef).find(Filters.eq("_id", dbRef.getId())).first();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.convert.DbRefResolver#bulkFetch(java.util.List)
	 */
	@Override
	public List<Document> bulkFetch(List<DBRef> refs) {

		Assert.notNull(mongoDbFactory, "Factory must not be null!");
		Assert.notNull(refs, "DBRef to fetch must not be null!");

		if (refs.isEmpty()) {
			return Collections.emptyList();
		}

		String collection = refs.iterator().next().getCollectionName();
		List<Object> ids = new ArrayList<>(refs.size());

		for (DBRef ref : refs) {

			if (!collection.equals(ref.getCollectionName())) {
				throw new InvalidDataAccessApiUsageException(
						"DBRefs must all target the same collection for bulk fetch operation.");
			}

			ids.add(ref.getId());
		}

		DBRef databaseSource = refs.iterator().next();

		if (LOGGER.isTraceEnabled()) {
			LOGGER.trace("Bulk fetching DBRefs {} from {}.{}.", ids,
					StringUtils.hasText(databaseSource.getDatabaseName()) ? databaseSource.getDatabaseName()
							: mongoDbFactory.getMongoDatabase().getName(),
					databaseSource.getCollectionName());
		}

		List<Document> result = getCollection(databaseSource) //
				.find(new Document("_id", new Document("$in", ids))) //
				.into(new ArrayList<>());

		return ids.stream() //
				.flatMap(id -> documentWithId(id, result)) //
				.collect(Collectors.toList());
	}

	/**
	 * Creates a proxy for the given {@link MongoPersistentProperty} using the given {@link DbRefResolverCallback} to
	 * eventually resolve the value of the property.
	 *
	 * @param property must not be {@literal null}.
	 * @param dbref can be {@literal null}.
	 * @param callback must not be {@literal null}.
	 * @return
	 */
	private Object createLazyLoadingProxy(MongoPersistentProperty property, @Nullable DBRef dbref,
			DbRefResolverCallback callback, DbRefProxyHandler handler) {

		Class<?> propertyType = property.getType();
		LazyLoadingInterceptor interceptor = new LazyLoadingInterceptor(property, dbref, exceptionTranslator, callback);

		if (!propertyType.isInterface()) {

			Factory factory = (Factory) objenesis.newInstance(getEnhancedTypeFor(propertyType));
			factory.setCallbacks(new Callback[] { interceptor });

			return handler.populateId(property, dbref, factory);
		}

		ProxyFactory proxyFactory = new ProxyFactory();

		for (Class<?> type : propertyType.getInterfaces()) {
			proxyFactory.addInterface(type);
		}

		proxyFactory.addInterface(LazyLoadingProxy.class);
		proxyFactory.addInterface(propertyType);
		proxyFactory.addAdvice(interceptor);

		return handler.populateId(property, dbref, proxyFactory.getProxy());
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

	/**
	 * Returns whether the property shall be resolved lazily.
	 *
	 * @param property must not be {@literal null}.
	 * @return
	 */
	private boolean isLazyDbRef(MongoPersistentProperty property) {
		return property.getDBRef() != null && property.getDBRef().lazy();
	}

	/**
	 * Returns document with the given identifier from the given list of {@link Document}s.
	 *
	 * @param identifier
	 * @param documents
	 * @return
	 */
	private static Stream<Document> documentWithId(Object identifier, Collection<Document> documents) {

		return documents.stream() //
				.filter(it -> it.get("_id").equals(identifier)) //
				.limit(1);
	}

	/**
	 * A {@link MethodInterceptor} that is used within a lazy loading proxy. The property resolving is delegated to a
	 * {@link DbRefResolverCallback}. The resolving process is triggered by a method invocation on the proxy and is
	 * guaranteed to be performed only once.
	 *
	 * @author Thomas Darimont
	 * @author Oliver Gierke
	 * @author Christoph Strobl
	 */
	static class LazyLoadingInterceptor
			implements MethodInterceptor, org.springframework.cglib.proxy.MethodInterceptor, Serializable {

		private static final Method INITIALIZE_METHOD, TO_DBREF_METHOD, FINALIZE_METHOD;

		private final DbRefResolverCallback callback;
		private final MongoPersistentProperty property;
		private final PersistenceExceptionTranslator exceptionTranslator;

		private volatile boolean resolved;
		private final @Nullable DBRef dbref;
		private @Nullable Object result;

		static {
			try {
				INITIALIZE_METHOD = LazyLoadingProxy.class.getMethod("getTarget");
				TO_DBREF_METHOD = LazyLoadingProxy.class.getMethod("toDBRef");
				FINALIZE_METHOD = Object.class.getDeclaredMethod("finalize");
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		/**
		 * Creates a new {@link LazyLoadingInterceptor} for the given {@link MongoPersistentProperty},
		 * {@link PersistenceExceptionTranslator} and {@link DbRefResolverCallback}.
		 *
		 * @param property must not be {@literal null}.
		 * @param dbref can be {@literal null}.
		 * @param callback must not be {@literal null}.
		 */
		public LazyLoadingInterceptor(MongoPersistentProperty property, @Nullable DBRef dbref,
				PersistenceExceptionTranslator exceptionTranslator, DbRefResolverCallback callback) {

			Assert.notNull(property, "Property must not be null!");
			Assert.notNull(exceptionTranslator, "Exception translator must not be null!");
			Assert.notNull(callback, "Callback must not be null!");

			this.dbref = dbref;
			this.callback = callback;
			this.exceptionTranslator = exceptionTranslator;
			this.property = property;
		}

		/*
		 * (non-Javadoc)
		 * @see org.aopalliance.intercept.MethodInterceptor#invoke(org.aopalliance.intercept.MethodInvocation)
		 */
		@Override
		public Object invoke(@Nullable MethodInvocation invocation) throws Throwable {
			return intercept(invocation.getThis(), invocation.getMethod(), invocation.getArguments(), null);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.cglib.proxy.MethodInterceptor#intercept(java.lang.Object, java.lang.reflect.Method, java.lang.Object[], org.springframework.cglib.proxy.MethodProxy)
		 */
		@Nullable
		@Override
		public Object intercept(Object obj, Method method, Object[] args, @Nullable MethodProxy proxy) throws Throwable {

			if (INITIALIZE_METHOD.equals(method)) {
				return ensureResolved();
			}

			if (TO_DBREF_METHOD.equals(method)) {
				return this.dbref;
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

		/**
		 * Returns a to string representation for the given {@code proxy}.
		 *
		 * @param proxy
		 * @return
		 */
		private String proxyToString(@Nullable Object proxy) {

			StringBuilder description = new StringBuilder();
			if (dbref != null) {
				description.append(dbref.getCollectionName());
				description.append(":");
				description.append(dbref.getId());
			} else {
				description.append(System.identityHashCode(proxy));
			}
			description.append("$").append(LazyLoadingProxy.class.getSimpleName());

			return description.toString();
		}

		/**
		 * Returns the hashcode for the given {@code proxy}.
		 *
		 * @param proxy
		 * @return
		 */
		private int proxyHashCode(@Nullable Object proxy) {
			return proxyToString(proxy).hashCode();
		}

		/**
		 * Performs an equality check for the given {@code proxy}.
		 *
		 * @param proxy
		 * @param that
		 * @return
		 */
		private boolean proxyEquals(@Nullable Object proxy, Object that) {

			if (!(that instanceof LazyLoadingProxy)) {
				return false;
			}

			if (that == proxy) {
				return true;
			}

			return proxyToString(proxy).equals(that.toString());
		}

		/**
		 * Will trigger the resolution if the proxy is not resolved already or return a previously resolved result.
		 *
		 * @return
		 */
		@Nullable
		private Object ensureResolved() {

			if (!resolved) {
				this.result = resolve();
				this.resolved = true;
			}

			return this.result;
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

		/**
		 * Resolves the proxy into its backing object.
		 *
		 * @return
		 */
		@Nullable
		private synchronized Object resolve() {

			if (resolved) {

				if (LOGGER.isTraceEnabled()) {
					LOGGER.trace("Accessing already resolved lazy loading property {}.{}",
							property.getOwner() != null ? property.getOwner().getName() : "unknown", property.getName());
				}
				return result;
			}

			try {
				if (LOGGER.isTraceEnabled()) {
					LOGGER.trace("Resolving lazy loading property {}.{}",
							property.getOwner() != null ? property.getOwner().getName() : "unknown", property.getName());
				}

				return callback.resolve(property);

			} catch (RuntimeException ex) {

				DataAccessException translatedException = this.exceptionTranslator.translateExceptionIfPossible(ex);

				if (translatedException instanceof ClientSessionException) {
					throw new LazyLoadingException("Unable to lazily resolve DBRef! Invalid session state.", ex);
				}

				throw new LazyLoadingException("Unable to lazily resolve DBRef!",
						translatedException != null ? translatedException : ex);
			}
		}
	}

	/**
	 * Customization hook for obtaining the {@link MongoCollection} for a given {@link DBRef}.
	 *
	 * @param dbref must not be {@literal null}.
	 * @return the {@link MongoCollection} the given {@link DBRef} points to.
	 * @since 2.1
	 */
	protected MongoCollection<Document> getCollection(DBRef dbref) {

		return (StringUtils.hasText(dbref.getDatabaseName()) ? mongoDbFactory.getDb(dbref.getDatabaseName())
				: mongoDbFactory.getMongoDatabase()).getCollection(dbref.getCollectionName(), Document.class);
	}
}
