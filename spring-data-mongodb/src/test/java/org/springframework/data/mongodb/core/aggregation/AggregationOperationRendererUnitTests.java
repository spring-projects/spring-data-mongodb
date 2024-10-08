/*
 * Copyright 2023-2024 the original author or authors.
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

import static org.mockito.Mockito.*;
import static org.springframework.data.domain.Sort.Direction.*;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.data.annotation.Id;
import org.springframework.data.convert.ConverterBuilder;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.convert.CustomConversions.StoreConversions;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.test.util.MongoTestMappingContext;

/**
 * @author Christoph Strobl
 */
public class AggregationOperationRendererUnitTests {

	@Test // GH-4443
	void nonFieldsExposingAggregationOperationContinuesWithSameContextForNextStage() {

		AggregationOperationContext rootContext = mock(AggregationOperationContext.class);
		AggregationOperation stage1 = mock(AggregationOperation.class);
		AggregationOperation stage2 = mock(AggregationOperation.class);

		AggregationOperationRenderer.toDocument(List.of(stage1, stage2), rootContext);

		verify(stage1).toPipelineStages(eq(rootContext));
		verify(stage2).toPipelineStages(eq(rootContext));
	}

	@Test // GH-4722
	void contextShouldCarryOnRelaxedFieldMapping() {

		MongoTestMappingContext ctx = new MongoTestMappingContext(cfg -> {
			cfg.initialEntitySet(TestRecord.class);
		});

		MappingMongoConverter mongoConverter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, ctx);

		Aggregation agg = Aggregation.newAggregation(Aggregation.unwind("layerOne.layerTwo"),
				project().and("layerOne.layerTwo.layerThree").as("layerOne.layerThree"),
				sort(DESC, "layerOne.layerThree.fieldA"));

		AggregationOperationRenderer.toDocument(agg.getPipeline().getOperations(),
				new RelaxedTypeBasedAggregationOperationContext(TestRecord.class, ctx, new QueryMapper(mongoConverter)));
	}

	@Test // GH-4722
	void appliesConversionToValuesUsedInAggregation() {

		MongoTestMappingContext ctx = new MongoTestMappingContext(cfg -> {
			cfg.initialEntitySet(TestRecord.class);
		});

		MappingMongoConverter mongoConverter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, ctx);
		mongoConverter.setCustomConversions(new CustomConversions(StoreConversions.NONE,
				Set.copyOf(ConverterBuilder.writing(ZonedDateTime.class, String.class, ZonedDateTime::toString)
						.andReading(it -> ZonedDateTime.parse(it)).getConverters())));
		mongoConverter.afterPropertiesSet();

		var agg = Aggregation.newAggregation(Aggregation.sort(Direction.DESC, "version"),
				Aggregation.group("entityId").first(Aggregation.ROOT).as("value"), Aggregation.replaceRoot("value"),
				Aggregation.match(Criteria.where("createdDate").lt(ZonedDateTime.now())) // here is the problem
		);

		List<Document> document = AggregationOperationRenderer.toDocument(agg.getPipeline().getOperations(),
				new RelaxedTypeBasedAggregationOperationContext(TestRecord.class, ctx, new QueryMapper(mongoConverter)));
		Assertions.assertThat(document).last()
				.extracting(it -> it.getEmbedded(List.of("$match", "createdDate", "$lt"), Object.class))
				.isInstanceOf(String.class);
	}

	@ParameterizedTest // GH-4722
	@MethodSource("studentAggregationContexts")
	void mapsOperationThatDoesNotExposeDedicatedFieldsCorrectly(AggregationOperationContext aggregationContext) {

		var agg = newAggregation(Student.class, Aggregation.unwind("grades"), Aggregation.replaceRoot("grades"),
				Aggregation.project("grades"));

		List<Document> mappedPipeline = AggregationOperationRenderer.toDocument(agg.getPipeline().getOperations(),
				aggregationContext);

		Assertions.assertThat(mappedPipeline).last().isEqualTo(Document.parse("{\"$project\": {\"grades\": 1}}"));
	}

	private static Stream<Arguments> studentAggregationContexts() {

		MongoTestMappingContext ctx = new MongoTestMappingContext(cfg -> {
			cfg.initialEntitySet(Student.class);
		});

		MappingMongoConverter mongoConverter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, ctx);
		mongoConverter.afterPropertiesSet();

		QueryMapper queryMapper = new QueryMapper(mongoConverter);

		return Stream.of(
				Arguments
						.of(new TypeBasedAggregationOperationContext(Student.class, ctx, queryMapper, FieldLookupPolicy.strict())),
				Arguments.of(
						new TypeBasedAggregationOperationContext(Student.class, ctx, queryMapper, FieldLookupPolicy.relaxed())));
	}

	record TestRecord(@Id String field1, String field2, LayerOne layerOne) {
		record LayerOne(List<LayerTwo> layerTwo) {
		}

		record LayerTwo(LayerThree layerThree) {
		}

		record LayerThree(int fieldA, int fieldB) {
		}
	}

	static class Student {

		@Field("mark") List<Grade> grades;

	}

	static class Grade {

		int points;
		String grades;
	}

}
