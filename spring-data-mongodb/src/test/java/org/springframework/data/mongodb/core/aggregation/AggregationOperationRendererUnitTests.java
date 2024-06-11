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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.data.domain.Sort.Direction.DESC;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.project;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.sort;

import java.util.List;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.aggregation.FieldsExposingAggregationOperation.InheritsFieldsAggregationOperation;
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

	@Test // GH-4443
	void fieldsExposingAggregationOperationNotExposingFieldsForcesUseOfDefaultContextForNextStage() {

		AggregationOperationContext rootContext = mock(AggregationOperationContext.class);
		FieldsExposingAggregationOperation stage1 = mock(FieldsExposingAggregationOperation.class);
		ExposedFields stage1fields = mock(ExposedFields.class);
		AggregationOperation stage2 = mock(AggregationOperation.class);

		when(stage1.getFields()).thenReturn(stage1fields);
		when(stage1fields.exposesNoFields()).thenReturn(true);

		AggregationOperationRenderer.toDocument(List.of(stage1, stage2), rootContext);

		verify(stage1).toPipelineStages(eq(rootContext));
		verify(stage2).toPipelineStages(eq(AggregationOperationRenderer.DEFAULT_CONTEXT));
	}

	@Test // GH-4443
	void fieldsExposingAggregationOperationForcesNewContextForNextStage() {

		AggregationOperationContext rootContext = mock(AggregationOperationContext.class);
		FieldsExposingAggregationOperation stage1 = mock(FieldsExposingAggregationOperation.class);
		ExposedFields stage1fields = mock(ExposedFields.class);
		AggregationOperation stage2 = mock(AggregationOperation.class);

		when(stage1.getFields()).thenReturn(stage1fields);
		when(stage1fields.exposesNoFields()).thenReturn(false);

		ArgumentCaptor<AggregationOperationContext> captor = ArgumentCaptor.forClass(AggregationOperationContext.class);

		AggregationOperationRenderer.toDocument(List.of(stage1, stage2), rootContext);

		verify(stage1).toPipelineStages(eq(rootContext));
		verify(stage2).toPipelineStages(captor.capture());

		assertThat(captor.getValue()).isInstanceOf(ExposedFieldsAggregationOperationContext.class)
				.isNotInstanceOf(InheritingExposedFieldsAggregationOperationContext.class);
	}

	@Test // GH-4443
	void inheritingFieldsExposingAggregationOperationForcesNewContextForNextStageKeepingReferenceToPreviousContext() {

		AggregationOperationContext rootContext = mock(AggregationOperationContext.class);
		InheritsFieldsAggregationOperation stage1 = mock(InheritsFieldsAggregationOperation.class);
		InheritsFieldsAggregationOperation stage2 = mock(InheritsFieldsAggregationOperation.class);
		InheritsFieldsAggregationOperation stage3 = mock(InheritsFieldsAggregationOperation.class);

		ExposedFields exposedFields = mock(ExposedFields.class);
		when(exposedFields.exposesNoFields()).thenReturn(false);
		when(stage1.getFields()).thenReturn(exposedFields);
		when(stage2.getFields()).thenReturn(exposedFields);
		when(stage3.getFields()).thenReturn(exposedFields);

		ArgumentCaptor<AggregationOperationContext> captor = ArgumentCaptor.forClass(AggregationOperationContext.class);

		AggregationOperationRenderer.toDocument(List.of(stage1, stage2, stage3), rootContext);

		verify(stage1).toPipelineStages(captor.capture());
		verify(stage2).toPipelineStages(captor.capture());
		verify(stage3).toPipelineStages(captor.capture());

		assertThat(captor.getAllValues().get(0)).isEqualTo(rootContext);

		assertThat(captor.getAllValues().get(1))
				.asInstanceOf(InstanceOfAssertFactories.type(InheritingExposedFieldsAggregationOperationContext.class))
				.extracting("previousContext").isSameAs(captor.getAllValues().get(0));

		assertThat(captor.getAllValues().get(2))
				.asInstanceOf(InstanceOfAssertFactories.type(InheritingExposedFieldsAggregationOperationContext.class))
				.extracting("previousContext").isSameAs(captor.getAllValues().get(1));
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
