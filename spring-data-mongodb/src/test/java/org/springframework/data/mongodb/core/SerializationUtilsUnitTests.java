/*
 * Copyright 2012-2023 the original author or authors.
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
import static org.springframework.data.mongodb.core.query.SerializationUtils.*;

import java.util.Arrays;
import java.util.Map;

import org.bson.Document;
import org.junit.jupiter.api.Test;
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
		assertThat(serializeToJsonSafely(document)).isEqualTo("{ \"foo\" : \"bar\"}");
	}

	@Test
	public void writesComplexObjectAsPlainToString() {

		Document document = new Document("foo", new Complex());
		assertThat(serializeToJsonSafely(document).startsWith(
				"{ \"foo\" : { \"$java\" : org.springframework.data.mongodb.core.SerializationUtilsUnitTests$Complex"));
	}

	@Test
	public void writesCollection() {

		Document document = new Document("foo", Arrays.asList("bar", new Complex()));
		assertThat(serializeToJsonSafely(document)).startsWith(
				"{ \"foo\" : [ \"bar\", { \"$java\" : org.springframework.data.mongodb.core.SerializationUtilsUnitTests$Complex")
				.endsWith(" } ] }");
	}

	@Test // DATAMONGO-1245
	public void flattenMapShouldFlatOutNestedStructureCorrectly() {

		Document document = new Document();
		document.put("_id", 1);
		document.put("nested", new Document("value", "conflux"));

		assertThat(flattenMap(document)).containsEntry("_id", 1).containsEntry("nested.value", "conflux");
	}

	@Test // DATAMONGO-1245
	public void flattenMapShouldFlatOutNestedStructureWithListCorrectly() {

		BasicDBList dbl = new BasicDBList();
		dbl.addAll(Arrays.asList("nightwielder", "calamity"));

		Document document = new Document();
		document.put("_id", 1);
		document.put("nested", new Document("value", dbl));

		assertThat(flattenMap(document)).containsEntry("_id", 1).containsEntry("nested.value", dbl);
	}

	@Test // DATAMONGO-1245
	public void flattenMapShouldLeaveKeywordsUntouched() {

		Document document = new Document();
		document.put("_id", 1);
		document.put("nested", new Document("$regex", "^conflux$"));

		Map<String, Object> map = flattenMap(document);

		assertThat(map).containsEntry("_id", 1).containsKey("nested");
		assertThat(((Map<String, Object>) map.get("nested")).get("$regex")).isEqualTo("^conflux$");
	}

	@Test // DATAMONGO-1245
	public void flattenMapShouldAppendCommandsCorrectly() {

		Document document = new Document();
		Document nested = new Document();
		nested.put("$regex", "^conflux$");
		nested.put("$options", "i");
		document.put("_id", 1);
		document.put("nested", nested);

		Map<String, Object> map = flattenMap(document);

		assertThat(map).containsEntry("_id", 1).containsKey("nested");
		assertThat(((Map<String, Object>) map.get("nested")).get("$regex")).isEqualTo("^conflux$");
		assertThat(((Map<String, Object>) map.get("nested")).get("$options")).isEqualTo("i");
	}

	@Test // DATAMONGO-1245
	public void flattenMapShouldReturnEmptyMapWhenSourceIsNull() {
		assertThat(flattenMap(null)).isEmpty();
	}

	static class Complex {

	}

}
