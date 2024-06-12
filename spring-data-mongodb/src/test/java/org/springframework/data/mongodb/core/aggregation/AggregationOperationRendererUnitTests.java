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

import java.util.List;

import org.junit.jupiter.api.Test;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.convert.QueryMapper;
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

	record TestRecord(@Id String field1, String field2, LayerOne layerOne) {
		record LayerOne(List<LayerTwo> layerTwo) {
		}

		record LayerTwo(LayerThree layerThree) {
		}

		record LayerThree(int fieldA, int fieldB)
		{}
	}

	@Test
	void xxx() {

		MongoTestMappingContext ctx = new MongoTestMappingContext(cfg -> {
			cfg.initialEntitySet(TestRecord.class);
		});

		MappingMongoConverter mongoConverter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, ctx);

		Aggregation agg = Aggregation.newAggregation(
			Aggregation.unwind("layerOne.layerTwo"),
			project().and("layerOne.layerTwo.layerThree").as("layerOne.layerThree"),
			sort(DESC, "layerOne.layerThree.fieldA")
		);

		AggregationOperationRenderer.toDocument(agg.getPipeline().getOperations(), new RelaxedTypeBasedAggregationOperationContext(TestRecord.class, ctx, new QueryMapper(mongoConverter)));
	}
}
