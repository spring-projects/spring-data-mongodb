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
package org.springframework.data.mongodb.core.aggregation;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.springframework.dao.InvalidDataAccessApiUsageException;

import com.mongodb.DBObject;

/**
 * Tests of {@link Projection}.
 * 
 * @see DATAMONGO-586
 * @author Tobias Trelle
 */
public class ProjectionTests {

	Projection projection;

	@Before
	public void setUp() {
		projection = new Projection();
	}

	@Test
	public void emptyProjection() {

		DBObject raw = projection.toDBObject();
		assertThat(raw, is(notNullValue()));
		assertThat(raw.toMap().isEmpty(), is(true));
	}

	@Test(expected = IllegalArgumentException.class)
	public void shouldDetectNullIncludesInConstructor() {
		new Projection((String[]) null);
	}

	@Test
	public void includesWithConstructor() {

		projection = new Projection("a", "b");

		DBObject raw = projection.toDBObject();
		assertThat(raw, is(notNullValue()));
		assertThat(raw.toMap().size(), is(3));
		assertThat((Integer) raw.get("_id"), is(0));
		assertThat((Integer) raw.get("a"), is(1));
		assertThat((Integer) raw.get("b"), is(1));
	}

	@Test
	public void include() {

		projection.include("a");

		DBObject raw = projection.toDBObject();
		assertSingleDBObject("a", 1, raw);
	}

	@Test
	public void exclude() {

		projection.exclude("a");

		DBObject raw = projection.toDBObject();
		assertSingleDBObject("a", 0, raw);
	}

	@Test
	public void includeAlias() {

		projection.include("a").as("b");

		DBObject raw = projection.toDBObject();
		assertSingleDBObject("b", "$a", raw);
	}

	@Test(expected = InvalidDataAccessApiUsageException.class)
	public void shouldDetectAliasWithoutInclude() {
		projection.as("b");
	}

	@Test(expected = InvalidDataAccessApiUsageException.class)
	public void shouldDetectDuplicateAlias() {
		projection.include("a").as("b").as("c");
	}

	@Test
	@SuppressWarnings("unchecked")
	public void plus() {

		projection.include("a").plus(10);

		DBObject raw = projection.toDBObject();
		assertNotNullDBObject(raw);

		DBObject addition = (DBObject) raw.get("a");
		assertNotNullDBObject(addition);

		List<Object> summands = (List<Object>) addition.get("$add");
		assertThat(summands, is(notNullValue()));
		assertThat(summands.size(), is(2));
		assertThat((String) summands.get(0), is("$a"));
		assertThat((Integer) summands.get(1), is(10));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void plusWithAlias() {

		projection.include("a").plus(10).as("b");

		DBObject raw = projection.toDBObject();
		assertNotNullDBObject(raw);

		DBObject addition = (DBObject) raw.get("b");
		assertNotNullDBObject(addition);

		List<Object> summands = (List<Object>) addition.get("$add");
		assertThat(summands, is(notNullValue()));
		assertThat(summands.size(), is(2));
		assertThat((String) summands.get(0), is("$a"));
		assertThat((Integer) summands.get(1), is(10));
	}

	private static void assertSingleDBObject(String key, Object value, DBObject doc) {

		assertNotNullDBObject(doc);
		assertThat(doc.get(key), is(value));
	}

	private static void assertNotNullDBObject(DBObject doc) {

		assertThat(doc, is(notNullValue()));
		assertThat(doc.toMap().size(), is(1));
	}
}
