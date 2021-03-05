/*
 * Copyright 2013-2021 the original author or authors.
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

import java.util.function.Consumer;

import org.springframework.aop.framework.Advised;
import org.springframework.cglib.proxy.Factory;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver.LazyLoadingInterceptor;
import org.springframework.data.mongodb.core.mapping.Unwrapped;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Utility class to test proxy handling for lazy loading.
 *
 * @author Oliver Gierke
 */
public class LazyLoadingTestUtils {

	/**
	 * Asserts that the given repository is resolved (expected is {@literal true}) and the value is non-{@literal null} or
	 * unresolved (expected is {@literal false}) and the value is {@literal null}.
	 *
	 * @param target
	 * @param expected
	 */
	public static void assertProxyIsResolved(Object target, boolean expected) {

		LazyLoadingInterceptor interceptor = extractInterceptor(target);
		assertThat(ReflectionTestUtils.getField(interceptor, "resolved")).isEqualTo((Object) expected);

		if (expected) {
			assertThat(ReflectionTestUtils.getField(interceptor, "result")).isNotNull();
		} else {
			assertThat(ReflectionTestUtils.getField(interceptor, "result")).isNull();

		}
	}

	public static void assertProxy(Object proxy, Consumer<LazyLoadingProxyValueRetriever> verification) {

		LazyLoadingProxyGenerator.LazyLoadingInterceptor interceptor = (LazyLoadingProxyGenerator.LazyLoadingInterceptor) (proxy instanceof Advised ? ((Advised) proxy).getAdvisors()[0].getAdvice()
				: ((Factory) proxy).getCallback(0));

		verification.accept(new LazyLoadingProxyValueRetriever(interceptor));
	}

	private static LazyLoadingInterceptor extractInterceptor(Object proxy) {
		return (LazyLoadingInterceptor) (proxy instanceof Advised ? ((Advised) proxy).getAdvisors()[0].getAdvice()
				: ((Factory) proxy).getCallback(0));
	}

	public static class LazyLoadingProxyValueRetriever {

		LazyLoadingProxyGenerator.LazyLoadingInterceptor interceptor;

		public LazyLoadingProxyValueRetriever(LazyLoadingProxyGenerator.LazyLoadingInterceptor interceptor) {
			this.interceptor = interceptor;
		}

		public boolean isResolved() {
			return (boolean) ReflectionTestUtils.getField(interceptor, "resolved");
		}

		@Unwrapped.Nullable
		public Object currentValue() {
			return ReflectionTestUtils.getField(interceptor, "result");
		}

	}
}
