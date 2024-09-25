/*
 * Copyright 2012-2024 the original author or authors.
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
package org.springframework.data.mongodb.config;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;

import com.mongodb.WriteConcern;

/**
 * Unit tests for {@link StringToWriteConcernConverter}.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
public class StringToWriteConcernConverterUnitTests {

	StringToWriteConcernConverter converter = new StringToWriteConcernConverter();

	@Test // DATAMONGO-2199
	public void createsWellKnownConstantsCorrectly() {
		assertThat(converter.convert("ACKNOWLEDGED")).isEqualTo(WriteConcern.ACKNOWLEDGED);
	}

	@Test
	public void createsWriteConcernForUnknownValue() {
		assertThat(converter.convert("-1")).isEqualTo(new WriteConcern("-1"));
	}
}
