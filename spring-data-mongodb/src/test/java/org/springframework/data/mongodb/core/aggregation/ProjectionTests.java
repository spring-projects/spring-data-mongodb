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
import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;

import java.util.List;

import org.junit.Test;
import org.springframework.dao.InvalidDataAccessApiUsageException;

import com.mongodb.DBObject;

/**
 * Tests of {@link ProjectionOperation}.
 * 
 * @see DATAMONGO-586
 * @author Tobias Trelle
 */
public class ProjectionTests {

	@Test
	public void emptyProjection() {

		DBObject raw = safeExtractDbObjectFromProjection(project());
		assertThat(raw.toMap().size(), is(1));
		assertThat((Integer) raw.get("_id"), is(0));
	}

	@Test(expected = IllegalArgumentException.class)
	public void shouldDetectNullIncludesInConstructor() {
		new ProjectionOperation((String[]) null);
	}

	@Test
	public void includesWithConstructor() {

		DBObject raw = safeExtractDbObjectFromProjection(project("a", "b"));
		assertThat(raw, is(notNullValue()));
		assertThat(raw.toMap().size(), is(3));
		assertThat((Integer) raw.get("_id"), is(0));
		assertThat((Integer) raw.get("a"), is(1));
		assertThat((Integer) raw.get("b"), is(1));
	}

	@Test
	public void include() {

		DBObject raw = safeExtractDbObjectFromProjection(project().include("a"));
		assertSingleDBObject("a", 1, raw);
	}

	@Test
	public void exclude() {

		DBObject raw = safeExtractDbObjectFromProjection(project().exclude("a"));
		assertThat(raw.toMap().size(), is(2));
		assertThat((Integer) raw.get("_id"), is(0));
		assertThat((Integer) raw.get("a"), is(0));

	}

	@Test
	public void includeAlias() {

		DBObject raw = safeExtractDbObjectFromProjection(project().include("a").as("b"));
		assertThat(raw.toMap().size(), is(2));
		assertThat((Integer) raw.get("_id"), is(0));
		assertThat((String) raw.get("b"), is("$a"));
	}

	@Test(expected = InvalidDataAccessApiUsageException.class)
	public void shouldDetectAliasWithoutInclude() {
		project().as("b");
	}

	@Test(expected = InvalidDataAccessApiUsageException.class)
	public void shouldDetectDuplicateAlias() {
		project().include("a").as("b").as("c");
	}

	@Test
	@SuppressWarnings("unchecked")
	public void plus() {

		DBObject raw = safeExtractDbObjectFromProjection(project().include("a").plus(10));
		assertThat(raw, is(notNullValue()));

		DBObject addition = (DBObject) raw.get("a");
		assertThat(addition, is(notNullValue()));

		List<Object> summands = (List<Object>) addition.get("$add");
		assertThat(summands, is(notNullValue()));
		assertThat(summands.size(), is(2));
		assertThat((String) summands.get(0), is("$a"));
		assertThat((Integer) summands.get(1), is(10));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void plusWithAlias() {

		DBObject raw = safeExtractDbObjectFromProjection(project().include("a").plus(10).as("b"));
		assertThat(raw, is(notNullValue()));

		DBObject addition = (DBObject) raw.get("b");
		assertThat(addition, is(notNullValue()));

		List<Object> summands = (List<Object>) addition.get("$add");
		assertThat(summands, is(notNullValue()));
		assertThat(summands.size(), is(2));
		assertThat((String) summands.get(0), is("$a"));
		assertThat((Integer) summands.get(1), is(10));
	}

	@Test
	public void projectionWithFields() {
		ProjectionOperation projectionOperation = project(ZipInfoStats.class) //
				.field("_id", 0) //
				.field("state", $id()) // $id() -> $_id
				.field("biggestCity", fields().and("name", $("biggestCity")).and("population", $("biggestPop"))) //
				.field("smallestCity", fields().and("name", $("smallestCity")).and("population", $("smallestPop")));

		assertThat(projectionOperation, is(notNullValue()));
	}

	private static DBObject safeExtractDbObjectFromProjection(ProjectionOperation projectionOperation) {

		assertThat(projectionOperation, is(notNullValue()));
		DBObject dbObject = projectionOperation.toDbObject(new BasicAggregateOperationContext());
		assertThat(dbObject, is(notNullValue()));
		Object projection = dbObject.get("$project");
		assertThat("Expected non null value for key $project ", projection, is(notNullValue()));
		assertTrue("projection contents should be a " + DBObject.class.getSimpleName(), projection instanceof DBObject);

		return DBObject.class.cast(projection);
	}

	private static void assertSingleDBObject(String key, Object value, DBObject doc) {

		assertThat(doc, is(notNullValue()));
		assertThat(doc.get(key), is(value));
	}
}
