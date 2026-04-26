/*
 * Copyright 2026 the original author or authors.
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
package org.springframework.data.mongodb.core.query;

import static org.assertj.core.api.Assertions.*;

import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SerializationUtils}.
 *
 * @author Your Full Name
 */
class SerializationUtilsUnitTests {

	@Test // GH-4873
	void serializesObjectIdUsingShellFormat() {

		ObjectId objectId = new ObjectId("507f1f77bcf86cd799439011");

		JsonWriterSettings settings = JsonWriterSettings.builder()
				.outputMode(JsonMode.SHELL)
				.build();

		assertThat(SerializationUtils.serializeToJsonSafely(new Document("_id", objectId), settings))
				.contains("ObjectId(\"507f1f77bcf86cd799439011\")")
				.doesNotContain("$oid");
	}
}