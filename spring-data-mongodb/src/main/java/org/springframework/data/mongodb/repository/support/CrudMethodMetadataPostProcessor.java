/*
 * Copyright 2023 the original author or authors.
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
package org.springframework.data.mongodb.repository.support;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.springframework.aop.TargetSource;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.core.NamedThreadLocal;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.support.RepositoryProxyPostProcessor;
import org.springframework.lang.Nullable;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

import com.mongodb.ReadPreference;

/**
 * {@link RepositoryProxyPostProcessor} that sets up interceptors to read metadata information from the invoked method.
 * This is necessary to allow redeclaration of CRUD methods in repository interfaces and configure read preference
 * information or query hints on them.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 4.2
 */
class CrudMethodMetadataPostProcessor implements RepositoryProxyPostProcessor, BeanClassLoaderAware {

	private @Nullable ClassLoader classLoader = ClassUtils.getDefaultClassLoader();

	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader;
	}

	@Override
	public void postProcess(ProxyFactory factory, RepositoryInformation repositoryInformation) {
		factory.addAdvice(new CrudMethodMetadataPopulatingMethodInterceptor(repositoryInformation));
	}

	/**
	 * Returns a {@link CrudMethodMetadata} proxy that will lookup the actual target object by obtaining a thread bound
	 * instance from the {@link TransactionSynchronizationManager} later.
	 */
	CrudMethodMetadata getCrudMethodMetadata() {

		ProxyFactory factory = new ProxyFactory();

		factory.addInterface(CrudMethodMetadata.class);
		factory.setTargetSource(new ThreadBoundTargetSource());

		return (CrudMethodMetadata) factory.getProxy(this.classLoader);
	}

	/**
	 * {@link MethodInterceptor} to build and cache {@link DefaultCrudMethodMetadata} instances for the invoked methods.
	 * Will bind the found information to a {@link TransactionSynchronizationManager} for later lookup.
	 *
	 * @see DefaultCrudMethodMetadata
	 */
	static class CrudMethodMetadataPopulatingMethodInterceptor implements MethodInterceptor {

		private static final ThreadLocal<MethodInvocation> currentInvocation = new NamedThreadLocal<>(
				"Current AOP method invocation");

		private final ConcurrentMap<Method, CrudMethodMetadata> metadataCache = new ConcurrentHashMap<>();
		private final Set<Method> implementations = new HashSet<>();
		private final RepositoryInformation repositoryInformation;

		CrudMethodMetadataPopulatingMethodInterceptor(RepositoryInformation repositoryInformation) {

			this.repositoryInformation = repositoryInformation;

			ReflectionUtils.doWithMethods(repositoryInformation.getRepositoryInterface(), implementations::add,
					method -> !repositoryInformation.isQueryMethod(method));
		}

		/**
		 * Return the AOP Alliance {@link MethodInvocation} object associated with the current invocation.
		 *
		 * @return the invocation object associated with the current invocation.
		 * @throws IllegalStateException if there is no AOP invocation in progress, or if the
		 *           {@link CrudMethodMetadataPopulatingMethodInterceptor} was not added to this interceptor chain.
		 */
		static MethodInvocation currentInvocation() throws IllegalStateException {

			MethodInvocation invocation = currentInvocation.get();

			if (invocation != null) {
				return invocation;
			}

			throw new IllegalStateException(
					"No MethodInvocation found: Check that an AOP invocation is in progress, and that the "
							+ "CrudMethodMetadataPopulatingMethodInterceptor is upfront in the interceptor chain.");
		}

		@Override
		public Object invoke(MethodInvocation invocation) throws Throwable {

			Method method = invocation.getMethod();

			if (!implementations.contains(method)) {
				return invocation.proceed();
			}

			MethodInvocation oldInvocation = currentInvocation.get();
			currentInvocation.set(invocation);

			try {

				CrudMethodMetadata metadata = (CrudMethodMetadata) TransactionSynchronizationManager.getResource(method);

				if (metadata != null) {
					return invocation.proceed();
				}

				CrudMethodMetadata methodMetadata = metadataCache.get(method);

				if (methodMetadata == null) {

					methodMetadata = new DefaultCrudMethodMetadata(repositoryInformation.getRepositoryInterface(), method);
					CrudMethodMetadata tmp = metadataCache.putIfAbsent(method, methodMetadata);

					if (tmp != null) {
						methodMetadata = tmp;
					}
				}

				TransactionSynchronizationManager.bindResource(method, methodMetadata);

				try {
					return invocation.proceed();
				} finally {
					TransactionSynchronizationManager.unbindResource(method);
				}
			} finally {
				currentInvocation.set(oldInvocation);
			}
		}
	}

	/**
	 * Default implementation of {@link CrudMethodMetadata} that will inspect the backing method for annotations.
	 */
	static class DefaultCrudMethodMetadata implements CrudMethodMetadata {

		private final Optional<ReadPreference> readPreference;

		/**
		 * Creates a new {@link DefaultCrudMethodMetadata} for the given {@link Method}.
		 *
		 * @param repositoryInterface the target repository interface.
		 * @param method must not be {@literal null}.
		 */
		DefaultCrudMethodMetadata(Class<?> repositoryInterface, Method method) {

			Assert.notNull(repositoryInterface, "Repository interface must not be null");
			Assert.notNull(method, "Method must not be null");

			this.readPreference = findReadPreference(method, repositoryInterface);
		}

		private static Optional<ReadPreference> findReadPreference(AnnotatedElement... annotatedElements) {

			for (AnnotatedElement element : annotatedElements) {

				org.springframework.data.mongodb.repository.ReadPreference preference = AnnotatedElementUtils
						.findMergedAnnotation(element, org.springframework.data.mongodb.repository.ReadPreference.class);

				if (preference != null) {
					return Optional.of(com.mongodb.ReadPreference.valueOf(preference.value()));
				}
			}

			return Optional.empty();
		}

		@Override
		public Optional<ReadPreference> getReadPreference() {
			return readPreference;
		}
	}

	private static class ThreadBoundTargetSource implements TargetSource {

		@Override
		public Class<?> getTargetClass() {
			return CrudMethodMetadata.class;
		}

		@Override
		public boolean isStatic() {
			return false;
		}

		@Override
		public Object getTarget() {

			MethodInvocation invocation = CrudMethodMetadataPopulatingMethodInterceptor.currentInvocation();
			return TransactionSynchronizationManager.getResource(invocation.getMethod());
		}

		@Override
		public void releaseTarget(Object target) {}
	}
}
