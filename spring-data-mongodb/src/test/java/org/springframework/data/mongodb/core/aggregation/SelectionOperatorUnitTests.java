/*
 * Copyright 2022-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.aggregation;

import static org.assertj.core.api.Assertions.*;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

/**
 * @author Christoph Strobl
 */
class SelectionOperatorUnitTests {

	@Test // GH-4139
	void bottomRenderedCorrectly() {

		Document document = SelectionOperators.Bottom.bottom().output(Fields.fields("playerId", "score"))
				.sortBy(Sort.by(Direction.DESC, "score")).toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(document).isEqualTo(Document.parse("""
				{
				   $bottom:
				   {
				      output: [ "$playerId", "$score" ],
				      sortBy: { "score": -1 }
				   }
				}
				"""));
	}

	@Test // GH-4139
	void bottomMapsFieldNamesCorrectly() {

		MongoMappingContext mappingContext = new MongoMappingContext();
		RelaxedTypeBasedAggregationOperationContext aggregationContext = new RelaxedTypeBasedAggregationOperationContext(
				Player.class, mappingContext,
				new QueryMapper(new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, mappingContext)));

		Document document = SelectionOperators.Bottom.bottom().output(Fields.fields("playerId", "score"))
				.sortBy(Sort.by(Direction.DESC, "score")).toDocument(aggregationContext);

		assertThat(document).isEqualTo(Document.parse("""
				{
				   $bottom:
				   {
				      output: [ "$player_id", "$s_cor_e" ],
				      sortBy: { "s_cor_e": -1 }
				   }
				}
				"""));
	}

	@Test // GH-4139
	void bottomNRenderedCorrectly() {

		Document document = SelectionOperators.Bottom.bottom(3).output(Fields.fields("playerId", "score"))
				.sortBy(Sort.by(Direction.DESC, "score")).toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(document).isEqualTo(Document.parse("""
				{
				   $bottomN:
				   {
				      n : 3,
				      output: [ "$playerId", "$score" ],
				      sortBy: { "score": -1 }
				   }
				}
				"""));
	}

	@Test // GH-4139
	void topMapsFieldNamesCorrectly() {

		MongoMappingContext mappingContext = new MongoMappingContext();
		RelaxedTypeBasedAggregationOperationContext aggregationContext = new RelaxedTypeBasedAggregationOperationContext(
				Player.class, mappingContext,
				new QueryMapper(new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, mappingContext)));

		Document document = SelectionOperators.Top.top().output(Fields.fields("playerId", "score"))
				.sortBy(Sort.by(Direction.DESC, "score")).toDocument(aggregationContext);

		assertThat(document).isEqualTo(Document.parse("""
				{
				   $top:
				   {
				      output: [ "$player_id", "$s_cor_e" ],
				      sortBy: { "s_cor_e": -1 }
				   }
				}
				"""));
	}

	@Test // GH-4139
	void topNRenderedCorrectly() {

		Document document = SelectionOperators.Top.top(3).output(Fields.fields("playerId", "score"))
				.sortBy(Sort.by(Direction.DESC, "score")).toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(document).isEqualTo(Document.parse("""
				{
				   $topN:
				   {
				      n : 3,
				      output: [ "$playerId", "$score" ],
				      sortBy: { "score": -1 }
				   }
				}
				"""));
	}

	@Test // GH-4139
	void firstNMapsFieldNamesCorrectly() {

		MongoMappingContext mappingContext = new MongoMappingContext();
		RelaxedTypeBasedAggregationOperationContext aggregationContext = new RelaxedTypeBasedAggregationOperationContext(
				Player.class, mappingContext,
				new QueryMapper(new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, mappingContext)));

		Document document = SelectionOperators.First.first(3).of("score").toDocument(aggregationContext);

		assertThat(document).isEqualTo(Document.parse("""
				{
				   $firstN:
				   {
				      n: 3,
				      input: "$s_cor_e"
				   }
				}
				"""));
	}

	@Test // GH-4139
	void lastNMapsFieldNamesCorrectly() {

		MongoMappingContext mappingContext = new MongoMappingContext();
		RelaxedTypeBasedAggregationOperationContext aggregationContext = new RelaxedTypeBasedAggregationOperationContext(
				Player.class, mappingContext,
				new QueryMapper(new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, mappingContext)));

		Document document = SelectionOperators.Last.last(3).of("score").toDocument(aggregationContext);

		assertThat(document).isEqualTo(Document.parse("""
				{
				   $lastN:
				   {
				      n: 3,
				      input: "$s_cor_e"
				   }
				}
				"""));
	}

	static class Player {

		@Field("player_id") String playerId;

		@Field("s_cor_e") Integer score;
	}
}
