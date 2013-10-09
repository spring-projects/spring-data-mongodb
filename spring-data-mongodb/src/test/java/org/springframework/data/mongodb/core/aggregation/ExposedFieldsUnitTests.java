/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.aggregation;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.junit.Test;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.ExposedField;

/**
 * Unit tests for {@link ExposedFields}.
 * 
 * @author Oliver Gierke
 * @author Thomas Darimont
 */
public class ExposedFieldsUnitTests {

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullFields() {
		ExposedFields.from((ExposedField) null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullFieldsForSynthetics() {
		ExposedFields.synthetic(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullFieldsForNonSynthetics() {
		ExposedFields.nonSynthetic(null);
	}

	@Test
	public void mitigateLeadingDollarSignInFieldName() {

		ExposedFields fields = ExposedFields.synthetic(Fields.fields("$foo"));
		assertThat(fields.iterator().next().getName(), is("$foo"));
		assertThat(fields.iterator().next().getTarget(), is("$foo"));
	}

	@Test
	public void exposesSingleField() {

		ExposedFields fields = ExposedFields.synthetic(Fields.fields("foo"));
		assertThat(fields.exposesSingleFieldOnly(), is(true));

		fields = fields.and(new ExposedField("bar", true));
		assertThat(fields.exposesSingleFieldOnly(), is(false));
	}
}
