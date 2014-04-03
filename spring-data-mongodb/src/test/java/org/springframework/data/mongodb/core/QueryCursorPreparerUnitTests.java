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

import static org.mockito.Mockito.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoTemplate.QueryCursorPreparer;
import org.springframework.data.mongodb.core.query.Query;

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

	/**
	 * @see DATAMONGO-185
	 */
	@Test
	public void appliesHintsCorrectly() {

		Query query = query(where("foo").is("bar")).withHint("hint");

		CursorPreparer preparer = new MongoTemplate(factory).new QueryCursorPreparer(query, null);
		preparer.prepare(cursor);

		verify(cursor).hint("hint");
	}
}
