/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.mongodb.core.convert;

import static org.assertj.core.api.Assertions.*;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.function.IntFunction;
import java.util.stream.Stream;

import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Unit tests for {@link DocumentIdIndex}.
 *
 * @author Christoph Strobl
 */
class DocumentIdIndexUnitTests {

	@ParameterizedTest(name = "{0}") // GH-5272
	@MethodSource("idTypes")
	void shouldMatchEqualIdInstances(String type, IntFunction<Object> id) {

		assertThat(id.apply(0)).isEqualTo(id.apply(0)).isNotSameAs(id.apply(0));

		Document one = new Document("_id", id.apply(0));
		Document two = new Document("_id", id.apply(1));
		Document three = new Document("_id", id.apply(2));
		DocumentIdIndex index = DocumentIdIndex.of(List.of(one, two, three));

		assertThat(index.select(List.of(id.apply(2), id.apply(0), id.apply(3), id.apply(1)))).containsExactly(three, one,
				two);
		assertThat(index.rank(List.of(new Document("_id", id.apply(2)), new Document("_id", id.apply(3)),
				new Document("_id", id.apply(0))))).containsExactly(2, 3, 0);
	}

	@Test // GH-5272
	void selectShouldFollowIdOrderSkippingMissingAndRepeatingDuplicates() {

		Document one = new Document("_id", 1);
		Document two = new Document("_id", 2);
		DocumentIdIndex index = DocumentIdIndex.of(List.of(one, two));

		assertThat(index.select(List.of(2, 3, 1, 2))).containsExactly(two, one, two);
	}

	@Test // GH-5272
	void selectShouldUseFirstDocumentForDuplicateIds() {

		Document first = new Document("_id", 1).append("name", "first");
		Document second = new Document("_id", 1).append("name", "second");

		assertThat(DocumentIdIndex.of(List.of(first, second)).select(List.of(1))).containsExactly(first);
	}

	@Test // GH-5272
	void getShouldReturnFirstDocumentOrNull() {

		Document first = new Document("_id", 1).append("name", "first");
		Document second = new Document("_id", 1).append("name", "second");
		DocumentIdIndex index = DocumentIdIndex.of(new LinkedList<>(List.of(first, second)));

		assertThat(index.get(1)).isSameAs(first);
		assertThat(index.get(2)).isNull();
	}

	@Test // GH-5272
	void selectShouldSupportSequentialAccessLists() {

		Document one = new Document("_id", 1);
		Document two = new Document("_id", 2);

		assertThat(DocumentIdIndex.of(new LinkedList<>(List.of(one, two))).select(List.of(2, 1))).containsExactly(two, one);
	}

	@Test // GH-5272
	void rankShouldUseFirstReferenceAndRankUnmatchedLast() {

		DocumentIdIndex index = DocumentIdIndex.of(List.of(new Document("_id", 1), new Document("_id", 2),
				new Document("_id", 3)));

		int[] ranks = index.rank(List.of(new Document("_id", 2), new Document("_id", 1), new Document("_id", 2)));

		assertThat(ranks).containsExactly(1, 0, 3);
	}

	@Test // GH-5272
	void rankShouldShareRankForDuplicateIds() {

		DocumentIdIndex index = DocumentIdIndex.of(List.of(new Document("_id", 1), new Document("_id", 2),
				new Document("_id", 1)));

		assertThat(index.rank(List.of(new Document("_id", 1), new Document("_id", 2)))).containsExactly(0, 1, 0);
	}

	@Test // GH-5272
	void rankShouldDistinguishMissingFromNullId() {

		DocumentIdIndex index = DocumentIdIndex.of(List.of(new Document("name", "none"), new Document("_id", null)));

		assertThat(index.rank(List.of(new Document("_id", null)))).containsExactly(1, 0);
	}

	static Stream<Arguments> idTypes() {

		return Stream.of( //
				idType("ObjectId", i -> new ObjectId("5f1d0c9a8e4b2a0012345" + (100 + i))), //
				idType("String", i -> new String("id-" + i)), //
				idType("Date", i -> new Date(1_700_000_000_000L + i)), //
				idType("Long", i -> Long.valueOf(1_000L + i)), //
				idType("UUID", i -> new UUID(42L, i)), //
				idType("Binary", i -> new Binary(new byte[] { 1, (byte) i })), //
				idType("byte[]", i -> new byte[] { 1, (byte) i }), //
				idType("Document", i -> new Document("tenant", "spring") //
						.append("created", new Date(1_700_000_000_000L)) //
						.append("sequence", Long.valueOf(1_000L + i)) //
						.append("nested", new Document("code", "code-" + i))));
	}

	private static Arguments idType(String type, IntFunction<Object> id) {
		return Arguments.of(type, id);
	}
}
