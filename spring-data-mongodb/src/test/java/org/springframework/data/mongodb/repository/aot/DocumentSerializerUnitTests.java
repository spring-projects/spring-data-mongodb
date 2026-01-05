/*
 * Copyright 2025-present the original author or authors.
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
package org.springframework.data.mongodb.repository.aot;

import static org.assertj.core.api.Assertions.*;

import java.util.List;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import org.springframework.data.mongodb.core.query.GeoCommand;

/**
 * Unit tests for {@link DocumentSerializer}.
 *
 * @author Mark Paluch
 */
class DocumentSerializerUnitTests {

	@Test // GH-5006
	void writesGeoCommandToJson() {

		Document source = new Document();
		source.append("foo", new GeoCommand(AotPlaceholders.box(1)));

		String jsonString = DocumentSerializer.toJson(source);

		assertThat(jsonString).isEqualTo("{\"foo\": {\"$box\": ?1}}");
	}

	@Test // GH-5006
	void writesPlaceholdersToJson() {

		Document source = new Document();
		source.append("foo", AotPlaceholders.indexed(1));
		source.append("someList", List.of("1", AotPlaceholders.indexed(2), 2, true));
		source.append("bar", AotPlaceholders.indexed(3));

		String jsonString = DocumentSerializer.toJson(source);

		assertThat(jsonString).isEqualTo("{\"foo\": ?1, \"someList\": [\"1\", ?2, 2, true], \"bar\": ?3}");
	}

}
