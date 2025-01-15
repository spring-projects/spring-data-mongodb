/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.mongodb.util.json;

import static org.assertj.core.api.Assertions.assertThat;

import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * @author Christoph Strobl
 * @since 2025/01
 */
public class SpringJsonWriterUnitTests {

	StringBuffer buffer;
	SpringJsonWriter writer;

	@BeforeEach
	void beforeEach() {
		buffer = new StringBuffer();
		writer = new SpringJsonWriter(buffer);
	}

	@Test
	void writeDocumentWithSingleEntry() {

		writer.writeStartDocument();
		writer.writeString("key", "value");
		writer.writeEndDocument();

		assertThat(buffer).isEqualToIgnoringWhitespace("{'key':'value'}");
	}

	@Test
	void writeDocumentWithMultipleEntries() {

		writer.writeStartDocument();
		writer.writeString("key-1", "v1");
		writer.writeString("key-2", "v2");
		writer.writeEndDocument();

		assertThat(buffer).isEqualToIgnoringWhitespace("{'key-1':'v1','key-2':'v2'}");
	}

	@Test
	void writeInt32() {

		writer.writeInt32("int32", 32);

		assertThat(buffer).isEqualToIgnoringWhitespace("'int32':{'$numberInt':'32'}");
	}

	@Test
	void writeInt64() {

		writer.writeInt64("int64", 64);

		assertThat(buffer).isEqualToIgnoringWhitespace("'int64':{'$numberLong':'64'}");
	}

	@Test
	void writeDouble() {

		writer.writeDouble("double", 42.24D);

		assertThat(buffer).isEqualToIgnoringWhitespace("'double':{'$numberDouble':'42.24'}");
	}

	@Test
	void writeDecimal128() {

		writer.writeDecimal128("decimal128", new Decimal128(128L));

		assertThat(buffer).isEqualToIgnoringWhitespace("'decimal128':{'$numberDecimal':'128'}");
	}

	@Test
	void writeObjectId() {

		ObjectId objectId = new ObjectId();
		writer.writeObjectId("_id", objectId);

		assertThat(buffer).isEqualToIgnoringWhitespace("'_id':{'$oid':'%s'}".formatted(objectId.toHexString()));
	}

	@Test
	void writeRegex() {

		String pattern = "^H";
		writer.writeRegularExpression("name", new BsonRegularExpression(pattern));

		assertThat(buffer).isEqualToIgnoringWhitespace("'name':{'$regex':/%s/}".formatted(pattern));
	}

	@Test
	void writeRegexWithOptions() {

		String pattern = "^H";
		writer.writeRegularExpression("name", new BsonRegularExpression(pattern, "i"));

		assertThat(buffer).isEqualToIgnoringWhitespace("'name':{'$regex':/%s/,'$options':'%s'}".formatted(pattern, "i"));
	}

	@Test
	void writeTimestamp() {

		writer.writeTimestamp("ts", new BsonTimestamp(1234, 567));

		assertThat(buffer).isEqualToIgnoringWhitespace("'ts':{'$timestamp':{'t':1234,'i':567}}");
	}

	@Test
	void writeUndefined() {

		writer.writeUndefined("nope");

		assertThat(buffer).isEqualToIgnoringWhitespace("'nope':{'$undefined':true}");
	}

	@Test
	void writeArrayWithSingleEntry() {

		writer.writeStartArray();
		writer.writeInt32(42);
		writer.writeEndArray();

		assertThat(buffer).isEqualToIgnoringNewLines("[{'$numberInt':'42'}]");
	}

	@Test
	void writeArrayWithMultipleEntries() {

		writer.writeStartArray();
		writer.writeInt32(42);
		writer.writeInt64(24);
		writer.writeEndArray();

		assertThat(buffer).isEqualToIgnoringNewLines("[{'$numberInt':'42'},{'$numberLong':'24'}]");
	}

}
