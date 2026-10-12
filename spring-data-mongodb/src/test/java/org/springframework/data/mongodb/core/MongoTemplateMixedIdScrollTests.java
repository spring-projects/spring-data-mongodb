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
package org.springframework.data.mongodb.core;

import static org.assertj.core.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.annotation.Id;
import org.springframework.data.domain.KeysetScrollPosition;
import org.springframework.data.domain.ScrollPosition;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Window;
import org.springframework.data.mongodb.core.mapping.MongoId;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.MongoTestTemplate;

import com.mongodb.client.MongoClient;

/**
 * Reproduction of keyset scrolling across BSON ID types.
 *
 * @author Priyanshu Gagiya
 */
class MongoTemplateMixedIdScrollTests {

	private static final String COLLECTION = "mixedIds";
	private static final String HEX_ONE = "000000000000000000000001";
	private static final String HEX_TWO = "000000000000000000000002";
	private static final ObjectId OBJECT_ONE = new ObjectId(HEX_ONE);
	private static final ObjectId OBJECT_TWO = new ObjectId(HEX_TWO);

	@Client
	static MongoClient client;

	MongoTestTemplate template = new MongoTestTemplate(client, "mixed-bson-keyset-scrolling-tests");

	@BeforeEach
	void setUp() {
		template.dropCollection(COLLECTION);
	}

	@AfterEach
	void tearDown() {
		template.dropCollection(COLLECTION);
	}

	@Test // GH-5260
	void keysetShouldTraverseMixedIdsAscending() {

		insertIds("A", "B", OBJECT_ONE, OBJECT_TWO);

		assertThat(scrollIds(Sort.Direction.ASC, ScrollPosition.keyset())).containsExactly("A", "B", OBJECT_ONE,
				OBJECT_TWO);
	}

	@Test // GH-5260
	void keysetShouldTraverseMixedIdsBackward() {

		insertIds("A", "B", OBJECT_ONE, OBJECT_TWO);

		assertThat(scrollIds(Sort.Direction.ASC, ScrollPosition.keyset().backward())).containsExactlyInAnyOrder(OBJECT_ONE,
				OBJECT_TWO, "A", "B");
	}

	@Test // GH-5260
	void keysetTraversesHomogeneousStringIds() {

		insertIds("A", "B", "C", "D");

		assertThat(scrollIds(Sort.Direction.ASC, ScrollPosition.keyset())).containsExactly("A", "B", "C", "D");
	}

	@Test // GH-5260
	void keysetTraversesHomogeneousObjectIds() {

		insertIds(OBJECT_ONE, OBJECT_TWO, new ObjectId("000000000000000000000003"));

		assertThat(scrollIds(Sort.Direction.ASC, ScrollPosition.keyset())).containsExactly(OBJECT_ONE, OBJECT_TWO,
				new ObjectId("000000000000000000000003"));
	}

	@Test // GH-5260
	void offsetTraversesMixedIds() {

		insertIds("A", "B", OBJECT_ONE, OBJECT_TWO);

		assertThat(scrollIds(Sort.Direction.ASC, ScrollPosition.offset())).containsExactly("A", "B", OBJECT_ONE,
				OBJECT_TWO);
	}

	@Test // GH-5260
	void streamTraversesMixedIds() {

		insertIds("A", "B", OBJECT_ONE, OBJECT_TWO);

		try (Stream<MixedId> stream = template.stream(new Query().with(Sort.by("id")), MixedId.class, COLLECTION)) {
			assertThat(stream.map(value -> {
				return value.id;
			}).toList()).containsExactly("A", "B", OBJECT_ONE, OBJECT_TWO);
		}
	}

	@Test // GH-5260
	void queryPredicateAndExpressionHaveDifferentTypeSemantics() {

		insertIds("A", "B", OBJECT_ONE, OBJECT_TWO);
		var collection = template.getCollection(COLLECTION);

		assertThat(collection.countDocuments(new Document("_id", new Document("$gt", "B")))).isZero();
		assertThat(collection.countDocuments(new Document("$expr", new Document("$gt", List.of("$_id", "B")))))
				.isEqualTo(2);
	}

	@Test // GH-5260
	void keysetPreservesHexadecimalStringsWithExplicitMongoIdMapping() {

		insertIds(HEX_ONE, HEX_TWO, "A");

		assertThat(scrollIds(Sort.Direction.ASC, ScrollPosition.keyset())).containsExactly(HEX_ONE, HEX_TWO, "A");
	}

	@Test // GH-5260
	void keysetShouldPreserveStoredHexadecimalStringWithDefaultIdMapping() {

		insertIds(HEX_ONE, HEX_TWO, "A");
		Query query = new Query().with(Sort.by("id")).limit(2).with(ScrollPosition.keyset());
		Window<StringId> first = template.scroll(query, StringId.class, COLLECTION);

		assertThat(first.hasNext()).isTrue();
		Window<StringId> second = template.scroll(query.with(first.positionAt(1)), StringId.class, COLLECTION);

		assertThat(second).extracting(value -> {
			return value.id;
		}).containsExactly("A");
	}

	private void insertIds(Object... ids) {

		List<Document> documents = new ArrayList<>();
		for (Object id : ids) {
			documents.add(new Document("_id", id));
		}
		template.getCollection(COLLECTION).insertMany(documents);
	}

	private List<Object> scrollIds(Sort.Direction direction, ScrollPosition position) {

		List<Object> result = new ArrayList<>();
		Query query = new Query().with(Sort.by(direction, "id")).limit(2);
		for (int page = 0; page < 10; page++) {
			Window<MixedId> window = template.scroll(query.with(position), MixedId.class, COLLECTION);
			for (MixedId value : window) {
				result.add(value.id);
			}
			if (!window.hasNext()) {
				return result;
			}
			boolean backward = position instanceof KeysetScrollPosition keyset
					&& keyset.getDirection() == ScrollPosition.Direction.BACKWARD;
			position = window.positionAt(backward ? 0 : window.size() - 1);
		}
		throw new AssertionError("Scrolling did not terminate within ten pages");
	}

	static class MixedId {
		@MongoId
		Object id;
	}

	static class StringId {
		@Id
		String id;
	}
}
