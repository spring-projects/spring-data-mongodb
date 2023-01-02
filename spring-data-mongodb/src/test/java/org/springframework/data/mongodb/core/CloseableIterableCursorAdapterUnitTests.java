/*
 * Copyright 2015-2023 the original author or authors.
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
import static org.mockito.Mockito.*;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.mongodb.core.MongoTemplate.CloseableIterableCursorAdapter;
import org.springframework.data.mongodb.core.MongoTemplate.DocumentCallback;
import org.springframework.data.util.CloseableIterator;

import com.mongodb.client.MongoCursor;

/**
 * Unit tests for {@link CloseableIterableCursorAdapter}.
 *
 * @author Oliver Gierke
 */
@ExtendWith(MockitoExtension.class)
class CloseableIterableCursorAdapterUnitTests {

	@Mock PersistenceExceptionTranslator exceptionTranslator;
	@Mock DocumentCallback<Object> callback;

	private MongoCursor<Document> cursor;
	private CloseableIterator<Object> adapter;

	@BeforeEach
	void setUp() {
		this.cursor = mock(MongoCursor.class);
		this.adapter = new CloseableIterableCursorAdapter<>(cursor, exceptionTranslator, callback);
	}

	@Test // DATAMONGO-1276
	void propagatesOriginalExceptionFromAdapterDotNext() {

		doThrow(IllegalArgumentException.class).when(cursor).next();
		assertThatIllegalArgumentException().isThrownBy(() -> adapter.next());
	}

	@Test // DATAMONGO-1276
	void propagatesOriginalExceptionFromAdapterDotHasNext() {

		doThrow(IllegalArgumentException.class).when(cursor).hasNext();
		assertThatIllegalArgumentException().isThrownBy(() -> adapter.hasNext());
	}

	@Test // DATAMONGO-1276
	void propagatesOriginalExceptionFromAdapterDotClose() {

		doThrow(IllegalArgumentException.class).when(cursor).close();
		assertThatIllegalArgumentException().isThrownBy(() -> adapter.close());
	}
}
