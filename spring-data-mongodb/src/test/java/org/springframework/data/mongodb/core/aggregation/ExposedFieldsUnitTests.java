/*
 * Copyright 2013-2023 the original author or authors.
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

import org.junit.jupiter.api.Test;

import org.springframework.data.mongodb.core.aggregation.ExposedFields.ExposedField;

/**
 * Unit tests for {@link ExposedFields}.
 *
 * @author Oliver Gierke
 * @author Thomas Darimont
 */
public class ExposedFieldsUnitTests {

	@Test
	public void rejectsNullFields() {
		assertThatIllegalArgumentException().isThrownBy(() -> ExposedFields.from((ExposedField) null));
	}

	@Test
	public void rejectsNullFieldsForSynthetics() {
		assertThatIllegalArgumentException().isThrownBy(() -> ExposedFields.synthetic(null));
	}

	@Test
	public void rejectsNullFieldsForNonSynthetics() {
		assertThatIllegalArgumentException().isThrownBy(() -> ExposedFields.nonSynthetic(null));
	}

	@Test
	public void exposesSingleField() {

		ExposedFields fields = ExposedFields.synthetic(Fields.fields("foo"));
		assertThat(fields.exposesSingleFieldOnly()).isTrue();

		fields = fields.and(new ExposedField("bar", true));
		assertThat(fields.exposesSingleFieldOnly()).isFalse();
	}
}
