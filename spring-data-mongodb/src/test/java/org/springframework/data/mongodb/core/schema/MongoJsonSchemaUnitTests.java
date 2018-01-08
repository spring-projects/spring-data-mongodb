/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.mongodb.core.schema;

import static org.springframework.data.mongodb.test.util.Assertions.*;

import java.util.Arrays;

import org.bson.Document;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class MongoJsonSchemaUnitTests {

	@Test // DATAMONGO-1835
	public void toDocumentRendersSchemaCorrectly() {

		MongoJsonSchema schema = MongoJsonSchema.builder() //
				.required("firstname", "lastname") //
				.build();

		assertThat(schema.toDocument()).isEqualTo(new Document("$jsonSchema",
				new Document("type", "object").append("required", Arrays.asList("firstname", "lastname"))));
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1835
	public void throwsExceptionOnNullRoot() {
		MongoJsonSchema.of(null);
	}
}
