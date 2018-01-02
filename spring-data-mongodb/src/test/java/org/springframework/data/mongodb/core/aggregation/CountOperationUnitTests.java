/*
 * Copyright 2016-2018 the original author or authors.
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

import org.bson.Document;
import org.junit.Test;

/**
 * Unit tests for {@link CountOperation}.
 *
 * @author Mark Paluch
 */
public class CountOperationUnitTests {

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1549
	public void rejectsEmptyFieldName() {
		new CountOperation("");
	}

	@Test // DATAMONGO-1549
	public void shouldRenderCorrectly() {

		CountOperation countOperation = new CountOperation("field");
		assertThat(countOperation.toDocument(Aggregation.DEFAULT_CONTEXT), is(Document.parse("{$count : \"field\" }")));
	}

	@Test // DATAMONGO-1549
	public void countExposesFields() {

		CountOperation countOperation = new CountOperation("field");

		assertThat(countOperation.getFields().exposesNoFields(), is(false));
		assertThat(countOperation.getFields().exposesSingleFieldOnly(), is(true));
		assertThat(countOperation.getFields().getField("field"), notNullValue());
	}
}
