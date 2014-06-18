/*
 * Copyright 2011-2014 the original author or authors.
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

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import org.hamcrest.core.Is;
import org.hamcrest.core.IsEqual;
import org.hamcrest.core.IsNot;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoTemplate.QueryCursorPreparer;
import org.springframework.data.mongodb.core.query.Query;

import com.mongodb.Cursor;
import com.mongodb.DBCursor;

/**
 * Unit tests for {@link QueryCursorPreparer}.
 * 
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class QueryCursorPreparerUnitTests {

	@Mock MongoDbFactory factory;
	@Mock DBCursor cursor;

	@Before
	public void setUp() {

		when(cursor.limit(anyInt())).thenReturn(cursor);
		when(cursor.skip(anyInt())).thenReturn(cursor);
	}

	/**
	 * @see DATAMONGO-185
	 */
	@Test
	public void appliesHintsCorrectly() {

		Query query = query(where("foo").is("bar")).withHint("hint");

		prepareCursor(query);

		verify(cursor).hint("hint");
	}

	/**
	 * @see DATAMONGO-950
	 */
	@Test
	public void shouldLimitCursorCorrectlyWhenMaxResultsSetAndNoLimitDefined() {

		Query query = query(where("foo").is("bar")).maxResults(100);

		prepareCursor(query);

		verify(cursor).limit(eq(100));
	}

	/**
	 * @see DATAMONGO-950
	 */
	@Test
	public void shouldLimitCursorCorrectlyWhenLimitLessThanMaxResults() {

		Query query = query(where("foo").is("bar")).limit(10).maxResults(100);

		prepareCursor(query);

		verify(cursor).limit(eq(10));
	}

	/**
	 * @see DATAMONGO-950
	 */
	@Test
	public void shouldLimitCursorCorrectlyWhenLimitAndOffsetGreaterThanMaxResults() {

		Query query = query(where("foo").is("bar")).limit(10).skip(95).maxResults(100);

		prepareCursor(query);

		verify(cursor).limit(eq(5));
	}

	/**
	 * @see DATAMONGO-950
	 */
	@Test
	public void shouldReturnEmptyCursorWhenOffsetExceedsMaxResults() {

		Cursor referenceCursor = cursor;
		Cursor cursorToUse = prepareCursor(query(where("foo").is("bar")).skip(101).maxResults(100));

		verify(cursor, never()).limit(anyInt());
		Assert.assertThat(cursorToUse, IsNot.not(IsEqual.equalTo(referenceCursor)));
		Assert.assertThat(cursorToUse.hasNext(), Is.is(false));

	}

	/**
	 * @see DATAMONGO-950
	 */
	@Test
	public void shouldNotLimitCursorWhenNoMaxResultSet() {

		prepareCursor(query(where("foo").is("bar")));
		verify(cursor, never()).limit(anyInt());
	}

	private Cursor prepareCursor(Query query) {
		CursorPreparer preparer = new MongoTemplate(factory).new QueryCursorPreparer(query, null);
		return preparer.prepare(cursor);
	}
}
