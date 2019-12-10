/*
 * Copyright 2011-2019 the original author or authors.
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

import static org.mockito.Mockito.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import java.util.concurrent.TimeUnit;

import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoTemplate.QueryCursorPreparer;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Query;

import com.mongodb.client.FindIterable;

/**
 * Unit tests for {@link QueryCursorPreparer}.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class QueryCursorPreparerUnitTests {

	@Mock MongoDbFactory factory;
	@Mock MongoExceptionTranslator exceptionTranslatorMock;
	@Mock FindIterable<Document> cursor;

	@Before
	public void setUp() {

		when(factory.getExceptionTranslator()).thenReturn(exceptionTranslatorMock);
		when(cursor.batchSize(anyInt())).thenReturn(cursor);
		when(cursor.comment(anyString())).thenReturn(cursor);
		when(cursor.maxTime(anyLong(), any())).thenReturn(cursor);
		when(cursor.hint(any())).thenReturn(cursor);
		when(cursor.noCursorTimeout(anyBoolean())).thenReturn(cursor);
		when(cursor.collation(any())).thenReturn(cursor);
	}

	@Test // DATAMONGO-185
	public void appliesHintsCorrectly() {

		Query query = query(where("foo").is("bar")).withHint("{ age: 1 }");
		prepare(query);

		verify(cursor).hint(new Document("age", 1));
	}

	@Test // DATAMONGO-2319
	public void appliesDocumentHintsCorrectly() {

		Query query = query(where("foo").is("bar")).withHint(Document.parse("{ age: 1 }"));
		prepare(query);

		verify(cursor).hint(new Document("age", 1));
	}

//	@Test // DATAMONGO-957
//	public void doesNotApplyMetaWhenEmpty() {
//
//		Query query = query(where("foo").is("bar"));
//		query.setMeta(new Meta());
//
//		prepare(query);
//
//		verify(cursor, never()).modifiers(any(Document.class));
//	}

	// @Test // DATAMONGO-957
	// public void appliesMaxScanCorrectly() {
	//
	// Query query = query(where("foo").is("bar")).maxScan(100);
	// prepare(query);
	//
	// verify(cursor).maxScan(100);
	// }

	@Test // DATAMONGO-957
	public void appliesMaxTimeCorrectly() {

		Query query = query(where("foo").is("bar")).maxTime(1, TimeUnit.SECONDS);
		prepare(query);

		verify(cursor).maxTime(1000, TimeUnit.MILLISECONDS);
	}

	@Test // DATAMONGO-957
	public void appliesCommentCorrectly() {

		Query query = query(where("foo").is("bar")).comment("spring data");
		prepare(query);

		verify(cursor).comment("spring data");
	}

//	@Test // DATAMONGO-957
//	public void appliesSnapshotCorrectly() {
//
//		Query query = query(where("foo").is("bar")).useSnapshot();
//		prepare(query);
//
//		verify(cursor).snapshot(true);
//	}

	@Test // DATAMONGO-1480
	public void appliesNoCursorTimeoutCorrectly() {

		Query query = query(where("foo").is("bar")).noCursorTimeout();

		prepare(query);

		verify(cursor).noCursorTimeout(eq(true));
	}

	@Test // DATAMONGO-1518
	public void appliesCollationCorrectly() {

		prepare(new BasicQuery("{}").collation(Collation.of("fr")));

		verify(cursor).collation(eq(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	@Test // DATAMONGO-1311
	public void appliesBatchSizeCorrectly() {

		prepare(new BasicQuery("{}").cursorBatchSize(100));

		verify(cursor).batchSize(100);
	}

	private FindIterable<Document> prepare(Query query) {

		CursorPreparer preparer = new MongoTemplate(factory).new QueryCursorPreparer(query, null);
		return preparer.prepare(cursor);
	}
}
