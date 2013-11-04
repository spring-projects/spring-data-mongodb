/*
 * Copyright 2013 the original author or authors.
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

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.springframework.aop.framework.Advised;
import org.springframework.cglib.proxy.Factory;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver.LazyLoadingInterceptor;

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
		assertThat(interceptor.isResolved(), is(expected));
		assertThat(interceptor.getResult(), is(expected ? notNullValue() : nullValue()));
	}

	private static LazyLoadingInterceptor extractInterceptor(Object proxy) {
		return (LazyLoadingInterceptor) (proxy instanceof Advised ? ((Advised) proxy).getAdvisors()[0].getAdvice()
				: ((Factory) proxy).getCallback(0));
	}
}
