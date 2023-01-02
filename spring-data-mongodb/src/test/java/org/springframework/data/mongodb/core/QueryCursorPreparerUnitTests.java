/*
 * Copyright 2011-2023 the original author or authors.
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

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate.QueryCursorPreparer;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Query;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.FindIterable;

/**
 * Unit tests for {@link QueryCursorPreparer}.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Anton Barkan
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class QueryCursorPreparerUnitTests {

	@Mock MongoDatabaseFactory factory;
	@Mock MongoExceptionTranslator exceptionTranslatorMock;
	@Mock FindIterable<Document> cursor;

	@BeforeEach
	void setUp() {

		when(factory.getExceptionTranslator()).thenReturn(exceptionTranslatorMock);
		when(factory.getCodecRegistry()).thenReturn(MongoClientSettings.getDefaultCodecRegistry());
		when(cursor.batchSize(anyInt())).thenReturn(cursor);
		when(cursor.comment(anyString())).thenReturn(cursor);
		when(cursor.allowDiskUse(anyBoolean())).thenReturn(cursor);
		when(cursor.maxTime(anyLong(), any())).thenReturn(cursor);
		when(cursor.hint(any())).thenReturn(cursor);
		when(cursor.noCursorTimeout(anyBoolean())).thenReturn(cursor);
		when(cursor.collation(any())).thenReturn(cursor);
	}

	@Test // DATAMONGO-185
	void appliesHintsCorrectly() {

		Query query = query(where("foo").is("bar")).withHint("{ age: 1 }");
		prepare(query);

		verify(cursor).hint(new Document("age", 1));
	}

	@Test // DATAMONGO-2365
	void appliesIndexNameAsHintCorrectly() {

		Query query = query(where("foo").is("bar")).withHint("idx-1");
		prepare(query);

		verify(cursor).hintString("idx-1");
	}

	@Test // DATAMONGO-2319
	void appliesDocumentHintsCorrectly() {

		Query query = query(where("foo").is("bar")).withHint(Document.parse("{ age: 1 }"));
		prepare(query);

		verify(cursor).hint(new Document("age", 1));
	}

	@Test // DATAMONGO-957
	void appliesCommentCorrectly() {

		Query query = query(where("foo").is("bar")).comment("spring data");
		prepare(query);

		verify(cursor).comment("spring data");
	}

	@Test // DATAMONGO-2659
	void appliesAllowDiskUseCorrectly() {

		Query query = query(where("foo").is("bar")).allowDiskUse(true);
		prepare(query);

		verify(cursor).allowDiskUse(true);
	}

	@Test // DATAMONGO-1480
	void appliesNoCursorTimeoutCorrectly() {

		Query query = query(where("foo").is("bar")).noCursorTimeout();

		prepare(query);

		verify(cursor).noCursorTimeout(eq(true));
	}

	@Test // DATAMONGO-1518
	void appliesCollationCorrectly() {

		prepare(new BasicQuery("{}").collation(Collation.of("fr")));

		verify(cursor).collation(eq(com.mongodb.client.model.Collation.builder().locale("fr").build()));
	}

	@Test // DATAMONGO-1311
	void appliesBatchSizeCorrectly() {

		prepare(new BasicQuery("{}").cursorBatchSize(100));

		verify(cursor).batchSize(100);
	}

	private FindIterable<Document> prepare(Query query) {

		CursorPreparer preparer = new MongoTemplate(factory).new QueryCursorPreparer(query, null);
		return preparer.prepare(cursor);
	}
}
