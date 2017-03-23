/*
 * Copyright 2015-2017 the original author or authors.
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

import static org.mockito.Mockito.*;

import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
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
@RunWith(MockitoJUnitRunner.class)
public class CloseableIterableCursorAdapterUnitTests {

	@Mock PersistenceExceptionTranslator exceptionTranslator;
	@Mock DocumentCallback<Object> callback;

	MongoCursor<Document> cursor;
	CloseableIterator<Object> adapter;

	@Before
	public void setUp() {

		this.cursor = doThrow(IllegalArgumentException.class).when(mock(MongoCursor.class));
		this.adapter = new CloseableIterableCursorAdapter<Object>(cursor, exceptionTranslator, callback);
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1276
	public void propagatesOriginalExceptionFromAdapterDotNext() {

		cursor.next();
		adapter.next();
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1276
	public void propagatesOriginalExceptionFromAdapterDotHasNext() {

		cursor.hasNext();
		adapter.hasNext();
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1276
	public void propagatesOriginalExceptionFromAdapterDotClose() {

		cursor.close();
		adapter.close();
	}
}
