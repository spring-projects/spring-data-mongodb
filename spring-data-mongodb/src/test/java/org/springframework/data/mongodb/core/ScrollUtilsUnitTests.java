/*
 * Copyright 2023-present the original author or authors.
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

import static org.assertj.core.api.AssertionsForClassTypes.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.KeysetScrollPosition;
import org.springframework.data.domain.ScrollPosition;
import org.springframework.data.domain.Window;
import org.springframework.data.mongodb.core.ScrollUtils.KeysetScrollResult;
import org.springframework.data.mongodb.core.query.Query;

/**
 * Unit tests for {@link ScrollUtils}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @author Priyanshu Gagiya
 */
class ScrollUtilsUnitTests {

	@Test // GH-5212
	void keysetCriteriaShouldNotMergeIntoExistingTopLevelOrPredicate() {

		Document baseQuery = new Document("$or",
				List.of(new Document("owner", "user1"), new Document("visible", true)));
		Document sortObject = new Document("score", 1).append("_id", 1);
		KeysetScrollPosition keyset = (KeysetScrollPosition) ScrollPosition.of(Map.of("score", 10, "_id", "abc"),
				ScrollPosition.Direction.FORWARD);

		Document result = ScrollUtils.KeysetScrollDirector.of(ScrollPosition.Direction.FORWARD).createQuery(keyset,
				baseQuery, sortObject, Function.identity());

		assertThat(result.containsKey("$and"))
				.as("query with a top-level $or must combine base criteria and keyset criteria with $and, "
						+ "not merge them into a single $or")
				.isTrue();

		@SuppressWarnings("unchecked")
		List<Document> andClauses = (List<Document>) result.get("$and");
		assertThat(andClauses).isNotNull();
		assertThat(andClauses.contains(baseQuery))
				.as("$and must contain the original base query as one of its arms").isTrue();
	}

	@Test // GH-4413
	void positionShouldRetainScrollDirection() {

		Query query = new Query();
		query.with(ScrollPosition.keyset().backward());
		Window<Integer> window = ScrollUtils.createWindow(query, new ArrayList<>(
				List.of(new KeysetScrollResult<>(1, Map.of("k", "v")), new KeysetScrollResult<>(2, Map.of("k", "w")))));

		assertThat(window.positionAt(0)).isInstanceOf(KeysetScrollPosition.class);
		assertThat(((KeysetScrollPosition) window.positionAt(0)).scrollsBackward()).isTrue();
	}
}
