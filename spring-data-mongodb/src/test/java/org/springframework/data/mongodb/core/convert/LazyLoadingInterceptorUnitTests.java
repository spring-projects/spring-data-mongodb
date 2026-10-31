/*
 * Copyright 2016-present the original author or authors.
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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.mongodb.LazyLoadingException;
import org.springframework.data.mongodb.core.convert.LazyLoadingProxyFactory.LazyLoadingInterceptor;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;

import com.mongodb.DBRef;

/**
 * Unit tests for {@link LazyLoadingInterceptor}.
 *
 * @author Christoph Strobl
 * @author Priyanshu Gagiya
 */
@ExtendWith(MockitoExtension.class)
class LazyLoadingInterceptorUnitTests {

	@Mock MongoPersistentProperty propertyMock;
	@Mock DBRef dbrefMock;
	@Mock DbRefResolverCallback callbackMock;

	@Test // DATAMONGO-1437
	void shouldPreserveCauseForNonTranslatableExceptions() throws Throwable {

		NullPointerException npe = new NullPointerException("Some Exception we did not think about.");
		when(callbackMock.resolve(propertyMock)).thenThrow(npe);

		assertThatExceptionOfType(LazyLoadingException.class).isThrownBy(() -> {
			new LazyLoadingInterceptor(propertyMock, callbackMock, dbrefMock, new NullExceptionTranslator()).intercept(null,
					LazyLoadingProxy.class.getMethod("getTarget"), null, null);
		}).withCause(npe);
	}

	@Test
	void shouldResolveOnlyOnceOnConcurrentAccess() throws Exception {

		ExecutorService executor = Executors.newFixedThreadPool(8);
		Method getTarget = LazyLoadingProxy.class.getMethod("getTarget");

		try {
			for (int round = 0; round < 25; round++) {
				CountDownLatch start = new CountDownLatch(1);
				AtomicInteger resolutions = new AtomicInteger();
				Object target = new Object();
				LazyLoadingInterceptor interceptor = new LazyLoadingInterceptor(propertyMock, property -> {
					resolutions.incrementAndGet();
					return target;
				}, dbrefMock, new NullExceptionTranslator());
				List<Future<Object>> results = new ArrayList<>();

				for (int worker = 0; worker < 8; worker++) {
					results.add(executor.submit(() -> {
						assertThat(start.await(5, TimeUnit.SECONDS)).isTrue();
						try {
							return interceptor.intercept(null, getTarget, null, null);
						} catch (Throwable failure) {
							throw new Exception(failure);
						}
					}));
				}

				start.countDown();
				for (Future<Object> result : results) {
					assertThat(result.get(5, TimeUnit.SECONDS)).isSameAs(target);
				}
				assertThat(resolutions.get()).isEqualTo(1);
			}
		} finally {
			executor.shutdownNow();
		}
	}

	static class NullExceptionTranslator implements PersistenceExceptionTranslator {

		@Override
		public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
			return null;
		}
	}
}
