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
package org.springframework.data.mongodb.core.query;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.mongodb.test.util.Assertions.*;
import static org.springframework.data.mongodb.test.util.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.data.mongodb.core.query.Update.Position;

/**
 * Unit tests for {@link BasicUpdate}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
class BasicUpdateUnitTests {

	@Test // GH-4918
	void setOperationValueShouldAppendsOpsCorrectly() {

		BasicUpdate basicUpdate = new BasicUpdate("{}");
		basicUpdate.setOperationValue("$set", "key1", "alt");
		basicUpdate.setOperationValue("$set", "key2", "nps");
		basicUpdate.setOperationValue("$unset", "key3", "x");

		assertThat(basicUpdate.getUpdateObject())
				.isEqualTo("{ '$set' : { 'key1' : 'alt', 'key2' : 'nps' }, '$unset' : { 'key3' : 'x' } }");
	}

	@Test // GH-4918
	void setOperationErrorsOnNonMapType() {

		BasicUpdate basicUpdate = new BasicUpdate("{ '$set' : 1 }");
		assertThatExceptionOfType(IllegalStateException.class)
				.isThrownBy(() -> basicUpdate.setOperationValue("$set", "k", "v"));
	}

	@ParameterizedTest // GH-4918
	@CsvSource({ //
			"{ }, k1, false", //
			"{ '$set' : { 'k1' : 'v1' } }, k1, true", //
			"{ '$set' : { 'k1' : 'v1' } }, k2, false", //
			"{ '$set' : { 'k1.k2' : 'v1' } }, k1, false", //
			"{ '$set' : { 'k1.k2' : 'v1' } }, k1.k2, true", //
			"{ '$set' : { 'k1' : 'v1' } }, '', false", //
			"{ '$inc' : { 'k1' : 1 } }, k1, true" })
	void modifiesLooksUpKeyCorrectly(String source, String key, boolean modified) {

		BasicUpdate basicUpdate = new BasicUpdate(source);
		assertThat(basicUpdate.modifies(key)).isEqualTo(modified);
	}

	@ParameterizedTest // GH-4918
	@MethodSource("updateOpArgs")
	void updateOpsShouldNotOverrideExistingValues(String operator, Function<BasicUpdate, Update> updateFunction) {

		Document source = Document.parse("{ '%s' : { 'key-1' : 'value-1' } }".formatted(operator));
		Update update = updateFunction.apply(new BasicUpdate(source));

		assertThat(update.getUpdateObject()).containsEntry("%s.key-1".formatted(operator), "value-1")
				.containsKey("%s.key-2".formatted(operator));
	}

	@Test // GH-4918
	void shouldNotOverridePullAll() {

		Document source = Document.parse("{ '$pullAll' : { 'key-1' : ['value-1'] } }");
		Update update = new BasicUpdate(source).pullAll("key-1", new String[] { "value-2" }).pullAll("key-2",
				new String[] { "value-3" });

		assertThat(update.getUpdateObject()).containsEntry("$pullAll.key-1", Arrays.asList("value-1", "value-2"))
				.containsEntry("$pullAll.key-2", List.of("value-3"));
	}

	static Stream<Arguments> updateOpArgs() {
		return Stream.of( //
				Arguments.of("$set", (Function<BasicUpdate, Update>) update -> update.set("key-2", "value-2")),
				Arguments.of("$unset", (Function<BasicUpdate, Update>) update -> update.unset("key-2")),
				Arguments.of("$inc", (Function<BasicUpdate, Update>) update -> update.inc("key-2", 1)),
				Arguments.of("$push", (Function<BasicUpdate, Update>) update -> update.push("key-2", "value-2")),
				Arguments.of("$addToSet", (Function<BasicUpdate, Update>) update -> update.addToSet("key-2", "value-2")),
				Arguments.of("$pop", (Function<BasicUpdate, Update>) update -> update.pop("key-2", Position.FIRST)),
				Arguments.of("$pull", (Function<BasicUpdate, Update>) update -> update.pull("key-2", "value-2")),
				Arguments.of("$pullAll",
						(Function<BasicUpdate, Update>) update -> update.pullAll("key-2", new String[] { "value-2" })),
				Arguments.of("$rename", (Function<BasicUpdate, Update>) update -> update.rename("key-2", "value-2")));
	};
}
