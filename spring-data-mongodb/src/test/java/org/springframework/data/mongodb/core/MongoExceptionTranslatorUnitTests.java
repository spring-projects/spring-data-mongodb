/*
 * Copyright 2013-2023 the original author or authors.
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
package org.springframework.data.mongodb.core;

import static org.assertj.core.api.Assertions.*;

import org.bson.BsonDocument;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.core.NestedRuntimeException;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.data.mongodb.ClientSessionException;
import org.springframework.data.mongodb.MongoTransactionException;
import org.springframework.data.mongodb.UncategorizedMongoDbException;
import org.springframework.lang.Nullable;

import com.mongodb.MongoCursorNotFoundException;
import com.mongodb.MongoException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoSocketReadTimeoutException;
import com.mongodb.MongoSocketWriteException;
import com.mongodb.ServerAddress;

/**
 * Unit tests for {@link MongoExceptionTranslator}.
 *
 * @author Michal Vich
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Brice Vandeputte
 */
class MongoExceptionTranslatorUnitTests {

	private static final String EXCEPTION_MESSAGE = "IOException";
	private MongoExceptionTranslator translator;

	@BeforeEach
	void setUp() {
		translator = new MongoExceptionTranslator();
	}

	@Test
	void translateDuplicateKey() {

		expectExceptionWithCauseMessage(
				translator.translateExceptionIfPossible(
						new com.mongodb.DuplicateKeyException(new BsonDocument(), new ServerAddress(), null)),
				DuplicateKeyException.class, null);
	}

	@Test // GH-3568
	void translateSocketException() {

		expectExceptionWithCauseMessage(
				translator.translateExceptionIfPossible(new MongoSocketException(EXCEPTION_MESSAGE, new ServerAddress())),
				DataAccessResourceFailureException.class, EXCEPTION_MESSAGE);
	}

	@Test // GH-3568
	void translateSocketExceptionSubclasses() {

		expectExceptionWithCauseMessage(
				translator.translateExceptionIfPossible(
						new MongoSocketWriteException("intermediate message", new ServerAddress(), new Exception(EXCEPTION_MESSAGE))
				),
				DataAccessResourceFailureException.class, EXCEPTION_MESSAGE);

		expectExceptionWithCauseMessage(
				translator.translateExceptionIfPossible(
						new MongoSocketReadTimeoutException("intermediate message", new ServerAddress(), new Exception(EXCEPTION_MESSAGE))
				),
				DataAccessResourceFailureException.class, EXCEPTION_MESSAGE);

	}

	@Test
	void translateCursorNotFound() {

		expectExceptionWithCauseMessage(
				translator.translateExceptionIfPossible(new MongoCursorNotFoundException(1L, new ServerAddress())),
				DataAccessResourceFailureException.class);
	}

	@Test
	void translateToDuplicateKeyException() {

		checkTranslatedMongoException(DuplicateKeyException.class, 11000);
		checkTranslatedMongoException(DuplicateKeyException.class, 11001);
	}

	@Test
	void translateToDataAccessResourceFailureException() {

		checkTranslatedMongoException(DataAccessResourceFailureException.class, 12000);
		checkTranslatedMongoException(DataAccessResourceFailureException.class, 13440);
	}

	@Test
	void translateToInvalidDataAccessApiUsageException() {

		checkTranslatedMongoException(InvalidDataAccessApiUsageException.class, 10003);
		checkTranslatedMongoException(InvalidDataAccessApiUsageException.class, 12001);
		checkTranslatedMongoException(InvalidDataAccessApiUsageException.class, 12010);
		checkTranslatedMongoException(InvalidDataAccessApiUsageException.class, 12011);
		checkTranslatedMongoException(InvalidDataAccessApiUsageException.class, 12012);
	}

	@Test
	void translateToUncategorizedMongoDbException() {

		MongoException exception = new MongoException(0, "");
		DataAccessException translatedException = translator.translateExceptionIfPossible(exception);

		expectExceptionWithCauseMessage(translatedException, UncategorizedMongoDbException.class);
	}

	@Test
	void translateMongoInternalException() {

		MongoInternalException exception = new MongoInternalException("Internal exception");
		DataAccessException translatedException = translator.translateExceptionIfPossible(exception);

		expectExceptionWithCauseMessage(translatedException, InvalidDataAccessResourceUsageException.class);
	}

	@Test
	void translateUnsupportedException() {

		RuntimeException exception = new RuntimeException();
		assertThat(translator.translateExceptionIfPossible(exception)).isNull();
	}

	@Test // DATAMONGO-2045
	void translateSessionExceptions() {

		checkTranslatedMongoException(ClientSessionException.class, 206);
		checkTranslatedMongoException(ClientSessionException.class, 213);
		checkTranslatedMongoException(ClientSessionException.class, 228);
		checkTranslatedMongoException(ClientSessionException.class, 264);
	}

	@Test // DATAMONGO-2045
	void translateTransactionExceptions() {

		checkTranslatedMongoException(MongoTransactionException.class, 217);
		checkTranslatedMongoException(MongoTransactionException.class, 225);
		checkTranslatedMongoException(MongoTransactionException.class, 244);
		checkTranslatedMongoException(MongoTransactionException.class, 251);
		checkTranslatedMongoException(MongoTransactionException.class, 256);
		checkTranslatedMongoException(MongoTransactionException.class, 257);
		checkTranslatedMongoException(MongoTransactionException.class, 263);
		checkTranslatedMongoException(MongoTransactionException.class, 267);
	}

	private void checkTranslatedMongoException(Class<? extends Exception> clazz, int code) {

		DataAccessException translated = translator.translateExceptionIfPossible(new MongoException(code, ""));

		assertThat(translated).as("Expected exception of type " + clazz.getName()).isNotNull();

		Throwable cause = translated.getRootCause();
		assertThat(cause).isInstanceOf(MongoException.class);
		assertThat(((MongoException) cause).getCode()).isEqualTo(code);
	}

	private static void expectExceptionWithCauseMessage(@Nullable NestedRuntimeException e,
			Class<? extends NestedRuntimeException> type) {
		expectExceptionWithCauseMessage(e, type, null);
	}

	private static void expectExceptionWithCauseMessage(@Nullable NestedRuntimeException e,
			Class<? extends NestedRuntimeException> type, @Nullable String message) {

		assertThat(e).isInstanceOf(type);

		if (message != null) {
			assertThat(e.getRootCause()).isNotNull();
			assertThat(e.getRootCause().getMessage()).contains(message);
		}
	}
}
