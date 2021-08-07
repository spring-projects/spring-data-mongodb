/*
 * Copyright 2017-2021 the original author or authors.
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

import org.bson.Document;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link SampleRateOperation}.
 *
 * @author James McNee
 */
public class SampleRateOperationUnitTests {

	private static final String OPERATION = "$sampleRate";

	@Test // GH-3726
	public void shouldRejectNegativeSampleRate() {
		assertThatIllegalArgumentException().isThrownBy(() -> new SampleRateOperation(-1.0));
	}

	@Test // GH-3726
	public void shouldRejectSampleRateGreaterThan1() {
		assertThatIllegalArgumentException().isThrownBy(() -> new SampleRateOperation(1.1));
	}

	@Test // GH-3726
	public void rendersSampleRateOperation() {
		double sampleRate = 0.34;

		SampleRateOperation sampleOperation = Aggregation.sampleRate(sampleRate);

		Document sampleOperationDocument = sampleOperation.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(sampleOperationDocument.get(OPERATION)).isNotNull();
		assertThat(sampleOperationDocument.get(OPERATION)).isEqualTo(sampleRate);
	}
}
