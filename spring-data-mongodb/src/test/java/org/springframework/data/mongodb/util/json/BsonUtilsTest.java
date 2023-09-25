/*
 * Copyright 2020-2023 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.bson.BsonArray;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.data.mongodb.core.mapping.FieldName;
import org.springframework.data.mongodb.util.BsonUtils;

import com.mongodb.BasicDBList;

/**
 * Unit tests for {@link BsonUtils}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
class BsonUtilsTest {

	@Test // DATAMONGO-625
	void simpleToBsonValue() {

		assertThat(BsonUtils.simpleToBsonValue(Long.valueOf(10))).isEqualTo(new BsonInt64(10));
		assertThat(BsonUtils.simpleToBsonValue(new Integer(10))).isEqualTo(new BsonInt32(10));
		assertThat(BsonUtils.simpleToBsonValue(Double.valueOf(0.1D))).isEqualTo(new BsonDouble(0.1D));
		assertThat(BsonUtils.simpleToBsonValue("value")).isEqualTo(new BsonString("value"));
	}

	@Test // DATAMONGO-625
	void primitiveToBsonValue() {
		assertThat(BsonUtils.simpleToBsonValue(10L)).isEqualTo(new BsonInt64(10));
	}

	@Test // DATAMONGO-625
	void objectIdToBsonValue() {

		ObjectId source = new ObjectId();
		assertThat(BsonUtils.simpleToBsonValue(source)).isEqualTo(new BsonObjectId(source));
	}

	@Test // DATAMONGO-625
	void bsonValueToBsonValue() {

		BsonObjectId source = new BsonObjectId(new ObjectId());
		assertThat(BsonUtils.simpleToBsonValue(source)).isSameAs(source);
	}

	@Test // DATAMONGO-625
	void unsupportedToBsonValue() {
		assertThatExceptionOfType(IllegalArgumentException.class)
				.isThrownBy(() -> BsonUtils.simpleToBsonValue(new Object()));
	}

	@Test // GH-3571
	void removeNullIdIfNull() {

		Document source = new Document("_id", null).append("value", "v-1");

		assertThat(BsonUtils.removeNullId(source)).isTrue();
		assertThat(source).doesNotContainKey("_id").containsKey("value");
	}

	@Test // GH-3571
	void removeNullIdDoesNotTouchNonNullOn() {

		Document source = new Document("_id", "id-value").append("value", "v-1");

		assertThat(BsonUtils.removeNullId(source)).isFalse();
		assertThat(source).containsKeys("_id", "value");
	}

	@Test // GH-3571
	void asCollectionDoesNotModifyCollection() {

		Object source = new ArrayList<>(0);

		assertThat(BsonUtils.asCollection(source)).isSameAs(source);
	}

	@Test // GH-3571
	void asCollectionConvertsArrayToCollection() {

		Object source = new String[] { "one", "two" };

		assertThat((Collection) BsonUtils.asCollection(source)).containsExactly("one", "two");
	}

	@Test // GH-3571
	void asCollectionConvertsWrapsNonIterable() {

		Object source = 100L;

		assertThat((Collection) BsonUtils.asCollection(source)).containsExactly(source);
	}

	@Test // GH-3702
	void supportsBsonShouldReportIfConversionSupported() {

		assertThat(BsonUtils.supportsBson("foo")).isFalse();
		assertThat(BsonUtils.supportsBson(new Document())).isTrue();
		assertThat(BsonUtils.supportsBson(new BasicDBList())).isTrue();
		assertThat(BsonUtils.supportsBson(Collections.emptyMap())).isTrue();
	}

	@ParameterizedTest // GH-4432
	@MethodSource("javaTimeInstances")
	void convertsJavaTimeTypesToBsonDateTime(Temporal source) {

		assertThat(BsonUtils.simpleToBsonValue(source))
				.isEqualTo(new Document("value", source).toBsonDocument().get("value"));
	}

	@ParameterizedTest // GH-4432
	@MethodSource("collectionLikeInstances")
	void convertsCollectionLikeToBsonArray(Object source) {

		assertThat(BsonUtils.simpleToBsonValue(source))
				.isEqualTo(new Document("value", source).toBsonDocument().get("value"));
	}

	@Test // GH-4432
	void convertsPrimitiveArrayToBsonArray() {

		assertThat(BsonUtils.simpleToBsonValue(new int[] { 1, 2, 3 }))
				.isEqualTo(new BsonArray(List.of(new BsonInt32(1), new BsonInt32(2), new BsonInt32(3))));
	}

	@ParameterizedTest
	@MethodSource("fieldNames")
	void resolveValueForField(FieldName fieldName, boolean exists) {

		Map<String, Object> source = new LinkedHashMap<>();
		source.put("a", "a-value"); // top level
		source.put("b", new Document("a", "b.a-value")); // path
		source.put("c.a", "c.a-value"); // key

		if(exists) {
			assertThat(BsonUtils.resolveValue(source, fieldName)).isEqualTo(fieldName.name() + "-value");
		} else {
			assertThat(BsonUtils.resolveValue(source, fieldName)).isNull();
		}
	}

	static Stream<Arguments> fieldNames() {
		return Stream.of(//
				Arguments.of(FieldName.path("a"), true), //
				Arguments.of(FieldName.path("b.a"), true), //
				Arguments.of(FieldName.path("c.a"), false), //
				Arguments.of(FieldName.name("d"), false), //
				Arguments.of(FieldName.name("b.a"), false), //
				Arguments.of(FieldName.name("c.a"), true) //
		);
	}

	static Stream<Arguments> javaTimeInstances() {

		return Stream.of(Arguments.of(Instant.now()), Arguments.of(LocalDate.now()), Arguments.of(LocalDateTime.now()),
				Arguments.of(LocalTime.now()));
	}

	static Stream<Arguments> collectionLikeInstances() {

		return Stream.of(Arguments.of(new String[] { "1", "2", "3" }), Arguments.of(List.of("1", "2", "3")),
				Arguments.of(new Integer[] { 1, 2, 3 }), Arguments.of(List.of(1, 2, 3)),
				Arguments.of(new Date[] { new Date() }), Arguments.of(List.of(new Date())),
				Arguments.of(new LocalDate[] { LocalDate.now() }), Arguments.of(List.of(LocalDate.now())));
	}
}
