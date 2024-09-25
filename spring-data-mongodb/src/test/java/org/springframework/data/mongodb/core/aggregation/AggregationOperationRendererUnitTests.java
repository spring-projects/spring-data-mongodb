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

import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.List;

import org.junit.jupiter.api.Test;

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
}
