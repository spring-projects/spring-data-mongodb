/*
 * Copyright 2013-2015 the original author or authors.
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
package org.springframework.data.mongodb.core;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.UnknownHostException;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.core.NestedRuntimeException;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.data.mongodb.UncategorizedMongoDbException;
import org.springframework.data.mongodb.util.MongoClientVersion;
import org.springframework.test.util.ReflectionTestUtils;

import com.mongodb.MongoCursorNotFoundException;
import com.mongodb.MongoException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoSocketException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcernException;

/**
 * Unit tests for {@link MongoExceptionTranslator}.
 *
 * @author Michal Vich
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class MongoExceptionTranslatorUnitTests {

	MongoExceptionTranslator translator;

	@Mock com.mongodb.DuplicateKeyException exception;
	@Mock MongoSocketException socketException;
	@Mock MongoCursorNotFoundException cursorNotFoundException;

	@Before
	public void setUp() {
		translator = new MongoExceptionTranslator();
	}

	@Test
	public void translateDuplicateKey() {

		DataAccessException translatedException = translator.translateExceptionIfPossible(exception);
		expectExceptionWithCauseMessage(translatedException, DuplicateKeyException.class, null);
	}

	@Test
	public void translateSocketException() {

		when(socketException.getMessage()).thenReturn("IOException");
		when(socketException.getCause()).thenReturn(new IOException("IOException"));
		DataAccessException translatedException = translator.translateExceptionIfPossible(socketException);

		expectExceptionWithCauseMessage(translatedException, DataAccessResourceFailureException.class, "IOException");

	}

	@Test
	public void translateCursorNotFound() throws UnknownHostException {

		when(cursorNotFoundException.getCode()).thenReturn(1);
		when(cursorNotFoundException.getServerAddress()).thenReturn(new ServerAddress());

		DataAccessException translatedException = translator.translateExceptionIfPossible(cursorNotFoundException);

		expectExceptionWithCauseMessage(translatedException, DataAccessResourceFailureException.class);
	}

	@Test
	public void translateToDuplicateKeyException() {

		checkTranslatedMongoException(DuplicateKeyException.class, 11000);
		checkTranslatedMongoException(DuplicateKeyException.class, 11001);
	}

	@Test
	public void translateToDataAccessResourceFailureException() {

		checkTranslatedMongoException(DataAccessResourceFailureException.class, 12000);
		checkTranslatedMongoException(DataAccessResourceFailureException.class, 13440);
	}

	@Test
	public void translateToInvalidDataAccessApiUsageException() {

		checkTranslatedMongoException(InvalidDataAccessApiUsageException.class, 10003);
		checkTranslatedMongoException(InvalidDataAccessApiUsageException.class, 12001);
		checkTranslatedMongoException(InvalidDataAccessApiUsageException.class, 12010);
		checkTranslatedMongoException(InvalidDataAccessApiUsageException.class, 12011);
		checkTranslatedMongoException(InvalidDataAccessApiUsageException.class, 12012);
	}

	@Test
	public void translateToUncategorizedMongoDbException() {

		MongoException exception = new MongoException(0, "");
		DataAccessException translatedException = translator.translateExceptionIfPossible(exception);

		expectExceptionWithCauseMessage(translatedException, UncategorizedMongoDbException.class);
	}

	@Test
	public void translateMongoInternalException() {

		MongoInternalException exception = new MongoInternalException("Internal exception");
		DataAccessException translatedException = translator.translateExceptionIfPossible(exception);

		expectExceptionWithCauseMessage(translatedException, InvalidDataAccessResourceUsageException.class);
	}

	@Test // DATAMONGO-1451
	@SuppressWarnings("unchecked")
	public void translateTimeoutToTransientDataAccessResourceExceptionWith2xDriver() throws Exception {

		assumeThat(MongoClientVersion.isMongo3Driver(), is(false));

		Constructor<?> constructor = Class.forName("com.mongodb.CommandResult").getDeclaredConstructor(ServerAddress.class);
		constructor.setAccessible(true);

		Map<String, Object> commandResult = (Map<String, Object>) constructor.newInstance(new ServerAddress("localhost"));
		commandResult.put("wtimeout", true);
		commandResult.put("ok", 1);
		commandResult.put("n", 0);
		commandResult.put("err", "waiting for replication timed out");
		commandResult.put("code", 64);

		DataAccessException translatedException = translator.translateExceptionIfPossible(
				(RuntimeException) ReflectionTestUtils.invokeMethod(commandResult, "getException"));

		expectExceptionWithCauseMessage(translatedException, TransientDataAccessResourceException.class);
	}

	@Test // DATAMONGO-1451
	public void translateTimeoutToTransientDataAccessResourceExceptionWith3xDriver() throws Exception {

		assumeThat(MongoClientVersion.isMongo3Driver(), is(true));

		Class<?> bsonDocumentClass = Class.forName("org.bson.BsonDocument");

		Method getWriteResult = Class.forName("com.mongodb.connection.ProtocolHelper").getDeclaredMethod("getWriteResult",
				bsonDocumentClass, ServerAddress.class);

		String response = "{ \"serverUsed\" : \"10.10.17.35:27017\" , \"ok\" : 1 , \"n\" : 0 , \"wtimeout\" : true , \"err\" : \"waiting for replication timed out\" , \"code\" : 64}";
		Object bsonDocument = bsonDocumentClass.getDeclaredMethod("parse", String.class).invoke(null, response);

		try {
			getWriteResult.setAccessible(true);
			getWriteResult.invoke(null, bsonDocument, new ServerAddress("localhost"));
			fail("Missing Exception");
		} catch (InvocationTargetException e) {

			assertThat(e.getTargetException(), is(instanceOf(WriteConcernException.class)));

			DataAccessException translatedException = translator
					.translateExceptionIfPossible((RuntimeException) e.getTargetException());
			expectExceptionWithCauseMessage(translatedException, TransientDataAccessResourceException.class);
		}

	}

	@Test
	public void translateUnsupportedException() {

		RuntimeException exception = new RuntimeException();
		assertThat(translator.translateExceptionIfPossible(exception), is(nullValue()));
	}

	private void checkTranslatedMongoException(Class<? extends Exception> clazz, int code) {

		try {
			translator.translateExceptionIfPossible(new MongoException(code, ""));
			fail("Expected exception of type " + clazz.getName() + "!");
		} catch (NestedRuntimeException e) {
			Throwable cause = e.getRootCause();
			assertThat(cause, is(instanceOf(MongoException.class)));
			assertThat(((MongoException) cause).getCode(), is(code));
		}
	}

	private static void expectExceptionWithCauseMessage(NestedRuntimeException e,
			Class<? extends NestedRuntimeException> type) {
		expectExceptionWithCauseMessage(e, type, null);
	}

	private static void expectExceptionWithCauseMessage(NestedRuntimeException e,
			Class<? extends NestedRuntimeException> type, String message) {

		assertThat(e, is(instanceOf(type)));

		if (message != null) {
			assertThat(e.getRootCause(), is(notNullValue()));
			assertThat(e.getRootCause().getMessage(), containsString(message));
		}
	}
}
