/*
 * Copyright 2012-2016 the original author or authors.
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

import org.bson.Document;
import org.hamcrest.Matcher;
import org.junit.Test;
import org.springframework.data.mongodb.core.query.SerializationUtils;

import com.mongodb.BasicDBList;

/**
 * Unit tests for {@link SerializationUtils}.
 * 
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
public class SerializationUtilsUnitTests {

	@Test
	public void writesSimpleDocument() {

		Document document = new Document("foo", "bar");
		assertThat(serializeToJsonSafely(document), is("{ \"foo\" : \"bar\"}"));
	}

	@Test
	public void writesComplexObjectAsPlainToString() {

		Document document = new Document("foo", new Complex());
		assertThat(serializeToJsonSafely(document),
				startsWith("{ \"foo\" : { $java : org.springframework.data.mongodb.core.SerializationUtilsUnitTests$Complex"));
	}

	@Test
	public void writesCollection() {

		Document document = new Document("foo", Arrays.asList("bar", new Complex()));
		Matcher<String> expectedOutput = allOf(
				startsWith(
						"{ \"foo\" : [ \"bar\", { $java : org.springframework.data.mongodb.core.SerializationUtilsUnitTests$Complex"),
				endsWith(" } ] }"));
		assertThat(serializeToJsonSafely(document), is(expectedOutput));
	}

	/**
	 * @see DATAMONGO-1245
	 */
	@Test
	public void flattenMapShouldFlatOutNestedStructureCorrectly() {

		Document document = new Document();
		document.put("_id", 1);
		document.put("nested", new Document("value", "conflux"));

		assertThat(flattenMap(document), hasEntry("_id", (Object) 1));
		assertThat(flattenMap(document), hasEntry("nested.value", (Object) "conflux"));
	}

	/**
	 * @see DATAMONGO-1245
	 */
	@Test
	public void flattenMapShouldFlatOutNestedStructureWithListCorrectly() {

		BasicDBList dbl = new BasicDBList();
		dbl.addAll(Arrays.asList("nightwielder", "calamity"));

		Document document = new Document();
		document.put("_id", 1);
		document.put("nested", new Document("value", dbl));

		assertThat(flattenMap(document), hasEntry("_id", (Object) 1));
		assertThat(flattenMap(document), hasEntry("nested.value", (Object) dbl));
	}

	/**
	 * @see DATAMONGO-1245
	 */
	@Test
	public void flattenMapShouldLeaveKeywordsUntouched() {

		Document document = new Document();
		document.put("_id", 1);
		document.put("nested", new Document("$regex", "^conflux$"));

		Map<String, Object> map = flattenMap(document);

		assertThat(map, hasEntry("_id", (Object) 1));
		assertThat(map.get("nested"), notNullValue());
		assertThat(((Map<String, Object>) map.get("nested")).get("$regex"), is((Object) "^conflux$"));
	}

	/**
	 * @see DATAMONGO-1245
	 */
	@Test
	public void flattenMapShouldAppendCommandsCorrectly() {

		Document document = new Document();
		Document nested = new Document();
		nested.put("$regex", "^conflux$");
		nested.put("$options", "i");
		document.put("_id", 1);
		document.put("nested", nested);

		Map<String, Object> map = flattenMap(document);

		assertThat(map, hasEntry("_id", (Object) 1));
		assertThat(map.get("nested"), notNullValue());
		assertThat(((Map<String, Object>) map.get("nested")).get("$regex"), is((Object) "^conflux$"));
		assertThat(((Map<String, Object>) map.get("nested")).get("$options"), is((Object) "i"));
	}

	/**
	 * @see DATAMONGO-1245
	 */
	@Test
	public void flattenMapShouldReturnEmptyMapWhenSourceIsNull() {
		assertThat(flattenMap(null).isEmpty(), is(true));
	}

	static class Complex {

	}

}
