/*
 * Copyright 2013-2016 the original author or authors.
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

import static org.springframework.util.ReflectionUtils.*;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.cglib.proxy.Callback;
import org.springframework.cglib.proxy.Enhancer;
import org.springframework.cglib.proxy.Factory;
import org.springframework.cglib.proxy.MethodProxy;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.mongodb.LazyLoadingException;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.objenesis.ObjenesisStd;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.DBRef;

/**
 * A {@link DbRefResolver} that resolves {@link org.springframework.data.mongodb.core.mapping.DBRef}s by delegating to a
 * {@link DbRefResolverCallback} than is able to generate lazy loading proxies.
 * 
 * @author Thomas Darimont
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @since 1.4
 */
public class DefaultDbRefResolver implements DbRefResolver {

	private static final String ID = "_id";

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
	public Object resolveDbRef(MongoPersistentProperty property, DBRef dbref, DbRefResolverCallback callback,
			DbRefProxyHandler handler) {

		Assert.notNull(property, "Property must not be null!");
		Assert.notNull(callback, "Callback must not be null!");

		if (isLazyDbRef(property)) {
			return createLazyLoadingProxy(property, dbref, callback, handler);
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
		return new DBRef(entity.getCollection(), id);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.convert.DbRefResolver#fetch(com.mongodb.DBRef)
	 */
	@Override
	public DBObject fetch(DBRef dbRef) {
		return ReflectiveDBRefResolver.fetch(mongoDbFactory, dbRef);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.convert.DbRefResolver#bulkFetch(java.util.List)
	 */
	@Override
	public List<DBObject> bulkFetch(List<DBRef> refs) {

		Assert.notNull(mongoDbFactory, "Factory must not be null!");
		Assert.notNull(refs, "DBRef to fetch must not be null!");

		if (refs.isEmpty()) {
			return Collections.emptyList();
		}

		String collection = refs.iterator().next().getCollectionName();

		List<Object> ids = new ArrayList<Object>(refs.size());
		for (DBRef ref : refs) {

			if (!collection.equals(ref.getCollectionName())) {
				throw new InvalidDataAccessApiUsageException(
						"DBRefs must all target the same collection for bulk fetch operation.");
			}

			ids.add(ref.getId());
		}

		Map<Object, DBObject> documentsById = getDocumentsById(ids, collection);
		List<DBObject> result = new ArrayList<DBObject>(ids.size());

		for (Object id : ids) {
			result.add(documentsById.get(id));
		}

		return result;
	}

	/**
	 * Returns all documents with the given ids contained in the given collection mapped by their ids.
	 * 
	 * @param ids must not be {@literal null}.
	 * @param collection must not be {@literal null} or empty.
	 * @return
	 */
	private Map<Object, DBObject> getDocumentsById(List<Object> ids, String collection) {

		Assert.notNull(ids, "Ids must not be null!");
		Assert.hasText(collection, "Collection must not be null or empty!");

		DB db = mongoDbFactory.getDb();
		BasicDBObject query = new BasicDBObject(ID, new BasicDBObject("$in", ids));
		List<DBObject> documents = db.getCollection(collection).find(query).toArray();
		Map<Object, DBObject> result = new HashMap<Object, DBObject>(documents.size());

		for (DBObject document : documents) {
			result.put(document.get(ID), document);
		}

		return result;
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
	private Object createLazyLoadingProxy(MongoPersistentProperty property, DBRef dbref, DbRefResolverCallback callback,
			DbRefProxyHandler handler) {

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
		private Object result;
		private DBRef dbref;

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
		public LazyLoadingInterceptor(MongoPersistentProperty property, DBRef dbref,
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
		public Object invoke(MethodInvocation invocation) throws Throwable {
			return intercept(invocation.getThis(), invocation.getMethod(), invocation.getArguments(), null);
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.cglib.proxy.MethodInterceptor#intercept(java.lang.Object, java.lang.reflect.Method, java.lang.Object[], org.springframework.cglib.proxy.MethodProxy)
		 */
		@Override
		public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {

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
		private String proxyToString(Object proxy) {

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
		private int proxyHashCode(Object proxy) {
			return proxyToString(proxy).hashCode();
		}

		/**
		 * Performs an equality check for the given {@code proxy}.
		 * 
		 * @param proxy
		 * @param that
		 * @return
		 */
		private boolean proxyEquals(Object proxy, Object that) {

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
		private synchronized Object resolve() {

			if (!resolved) {

				try {

					return callback.resolve(property);

				} catch (RuntimeException ex) {

					DataAccessException translatedException = this.exceptionTranslator.translateExceptionIfPossible(ex);
					throw new LazyLoadingException("Unable to lazily resolve DBRef!",
							translatedException != null ? translatedException : ex);
				}
			}

			return result;
		}
	}

	/**
	 * {@link Comparator} for sorting {@link DBObject} that have been loaded in random order by a predefined list of
	 * reference identifiers.
	 *
	 * @author Christoph Strobl
	 * @author Oliver Gierke
	 * @since 1.10
	 */
	private static class DbRefByReferencePositionComparator implements Comparator<DBObject> {

		private final List<Object> reference;

		/**
		 * Creates a new {@link DbRefByReferencePositionComparator} for the given list of reference identifiers.
		 * 
		 * @param referenceIds must not be {@literal null}.
		 */
		public DbRefByReferencePositionComparator(List<Object> referenceIds) {

			Assert.notNull(referenceIds, "Reference identifiers must not be null!");
			this.reference = new ArrayList<Object>(referenceIds);
		}

		/*
		 * (non-Javadoc)
		 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
		 */
		@Override
		public int compare(DBObject o1, DBObject o2) {
			return Integer.compare(reference.indexOf(o1.get(ID)), reference.indexOf(o2.get(ID)));
		}
	}
}
