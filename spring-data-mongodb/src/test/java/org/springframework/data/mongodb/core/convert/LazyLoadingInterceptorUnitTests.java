/*
 * Copyright 2016-2017 the original author or authors.
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

import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsEqual.*;
import static org.mockito.Mockito.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.mongodb.LazyLoadingException;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver.LazyLoadingInterceptor;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;

import com.mongodb.DBRef;

/**
 * Unit tests for {@link LazyLoadingInterceptor}.
 *
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class LazyLoadingInterceptorUnitTests {

	public @Rule ExpectedException exception = ExpectedException.none();

	@Mock MongoPersistentProperty propertyMock;
	@Mock DBRef dbrefMock;
	@Mock DbRefResolverCallback callbackMock;

	@Test // DATAMONGO-1437
	public void shouldPreserveCauseForNonTranslatableExceptions() throws Throwable {

		NullPointerException npe = new NullPointerException("Some Exception we did not think about.");
		when(callbackMock.resolve(propertyMock)).thenThrow(npe);

		exception.expect(LazyLoadingException.class);
		exception.expectCause(is(equalTo(npe)));

		new LazyLoadingInterceptor(propertyMock, dbrefMock, new NullExceptionTranslator(), callbackMock).intercept(null,
				LazyLoadingProxy.class.getMethod("getTarget"), null, null);
	}

	static class NullExceptionTranslator implements PersistenceExceptionTranslator {

		@Override
		public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
			return null;
		}
	}
}
