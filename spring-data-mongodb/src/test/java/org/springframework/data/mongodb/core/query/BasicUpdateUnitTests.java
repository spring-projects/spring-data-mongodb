/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.mongodb.core.query;

import static org.springframework.data.mongodb.test.util.Assertions.assertThat;

import java.util.function.Function;
import java.util.stream.Stream;

import org.bson.Document;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.data.mongodb.core.query.Update.Position;

/**
 * @author Christoph Strobl
 */
public class BasicUpdateUnitTests {

	@ParameterizedTest // GH-4918
	@MethodSource("args")
	void updateOpsShouldNotOverrideExistingValues(String operator, Function<BasicUpdate, Update> updateFunction) {

		Document source = Document.parse("{ '%s' : { 'key-1' : 'value-1' } }".formatted(operator));
		Update update = updateFunction.apply(new BasicUpdate(source));

		assertThat(update.getUpdateObject()).containsEntry("%s.key-1".formatted(operator), "value-1")
				.containsKey("%s.key-2".formatted(operator));
	}

	static Stream<Arguments> args() {
		return Stream.of( //
				Arguments.of("$set", (Function<BasicUpdate, Update>) update -> update.set("key-2", "value-2")),
				Arguments.of("$inc", (Function<BasicUpdate, Update>) update -> update.inc("key-2", 1)),
				Arguments.of("$push", (Function<BasicUpdate, Update>) update -> update.push("key-2", "value-2")),
				Arguments.of("$addToSet", (Function<BasicUpdate, Update>) update -> update.addToSet("key-2", "value-2")),
				Arguments.of("$pop", (Function<BasicUpdate, Update>) update -> update.pop("key-2", Position.FIRST)),
				Arguments.of("$pull", (Function<BasicUpdate, Update>) update -> update.pull("key-2", "value-2")),
				Arguments.of("$rename", (Function<BasicUpdate, Update>) update -> update.rename("key-2", "value-2")));
	};
}
