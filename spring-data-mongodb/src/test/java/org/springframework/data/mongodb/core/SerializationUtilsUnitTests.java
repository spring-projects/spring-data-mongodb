/*
 * Copyright 2012-2015 the original author or authors.
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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.springframework.data.mongodb.core.query.SerializationUtils.*;

import java.util.Arrays;
import java.util.Map;

import org.hamcrest.Matcher;
import org.hamcrest.collection.IsMapContaining;
import org.hamcrest.core.Is;
import org.junit.Test;
import org.springframework.data.mongodb.core.query.SerializationUtils;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;

/**
 * Unit tests for {@link SerializationUtils}.
 * 
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
public class SerializationUtilsUnitTests {

	@Test
	public void writesSimpleDBObject() {

		DBObject dbObject = new BasicDBObject("foo", "bar");
		assertThat(serializeToJsonSafely(dbObject), is("{ \"foo\" : \"bar\"}"));
	}

	@Test
	public void writesComplexObjectAsPlainToString() {

		DBObject dbObject = new BasicDBObject("foo", new Complex());
		assertThat(serializeToJsonSafely(dbObject),
				startsWith("{ \"foo\" : { $java : org.springframework.data.mongodb.core.SerializationUtilsUnitTests$Complex"));
	}

	@Test
	public void writesCollection() {

		DBObject dbObject = new BasicDBObject("foo", Arrays.asList("bar", new Complex()));
		Matcher<String> expectedOutput = allOf(
				startsWith("{ \"foo\" : [ \"bar\", { $java : org.springframework.data.mongodb.core.SerializationUtilsUnitTests$Complex"),
				endsWith(" } ] }"));
		assertThat(serializeToJsonSafely(dbObject), is(expectedOutput));
	}

	/**
	 * @see DATAMONGO-1245
	 */
	@Test
	public void flatMapShouldFlatOutNestedStructureCorrectly() {

		DBObject dbo = new BasicDBObjectBuilder().add("_id", 1).add("nested", new BasicDBObject("value", "conflux")).get();

		assertThat(flatMap(dbo), IsMapContaining.<String, Object> hasEntry("_id", 1));
		assertThat(flatMap(dbo), IsMapContaining.<String, Object> hasEntry("nested.value", "conflux"));
	}

	/**
	 * @see DATAMONGO-1245
	 */
	@Test
	public void flatMapShouldFlatOutNestedStructureWithListCorrectly() {

		BasicDBList dbl = new BasicDBList();
		dbl.addAll(Arrays.asList("nightwielder", "calamity"));

		DBObject dbo = new BasicDBObjectBuilder().add("_id", 1).add("nested", new BasicDBObject("value", dbl)).get();

		assertThat(flatMap(dbo), IsMapContaining.<String, Object> hasEntry("_id", 1));
		assertThat(flatMap(dbo), IsMapContaining.<String, Object> hasEntry("nested.value", dbl));
	}

	/**
	 * @see DATAMONGO-1245
	 */
	@Test
	public void flatMapShouldLeaveKeywordsUntouched() {

		DBObject dbo = new BasicDBObjectBuilder().add("_id", 1).add("nested", new BasicDBObject("$regex", "^conflux$"))
				.get();

		Map<String, Object> map = flatMap(dbo);

		assertThat(map, IsMapContaining.<String, Object> hasEntry("_id", 1));
		assertThat(map.get("nested"), notNullValue());
		assertThat(((Map<String, Object>) map.get("nested")).get("$regex"), Is.<Object> is("^conflux$"));
	}

	/**
	 * @see DATAMONGO-1245
	 */
	@Test
	public void flatMapSouldAppendCommandsCorrectly() {

		DBObject dbo = new BasicDBObjectBuilder().add("_id", 1)
				.add("nested", new BasicDBObjectBuilder().add("$regex", "^conflux$").add("$options", "i").get()).get();

		Map<String, Object> map = flatMap(dbo);

		assertThat(map, IsMapContaining.<String, Object> hasEntry("_id", 1));
		assertThat(map.get("nested"), notNullValue());
		assertThat(((Map<String, Object>) map.get("nested")).get("$regex"), Is.<Object> is("^conflux$"));
		assertThat(((Map<String, Object>) map.get("nested")).get("$options"), Is.<Object> is("i"));
	}

	/**
	 * @see DATAMONGO-1245
	 */
	@Test
	public void flatMapShouldReturnEmptyMapWhenSourceIsNull() {
		assertThat(flatMap(null).isEmpty(), is(true));
	}

	static class Complex {

	}

}
