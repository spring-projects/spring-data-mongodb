/*
 * Copyright 2019-2023 the original author or authors.
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

/**
 * Unit tests for {@link AggregationUpdate}.
 *
 * @author Christoph Strobl
 */
public class AggregationUpdateUnitTests {

	@Test // DATAMONGO-2331
	public void createPipelineWithMultipleStages() {

		assertThat(AggregationUpdate.update() //
				.set("stage-1").toValue("value-1") //
				.unset("stage-2") //
				.set("stage-3").toValue("value-3") //
				.toPipeline(Aggregation.DEFAULT_CONTEXT)) //
						.containsExactly(new Document("$set", new Document("stage-1", "value-1")),
								new Document("$unset", "stage-2"), new Document("$set", new Document("stage-3", "value-3")));
	}
}
