/*
 * Copyright 2016 the original author or authors.
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

import static org.junit.Assert.*;
import static org.springframework.data.mongodb.test.util.IsBsonObject.*;

import java.util.Arrays;

import org.junit.Test;

import com.mongodb.DBObject;

/**
 * Unit tests for {@link IfNullOperator}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
public class IfNullOperatorUnitTests {

	/**
	 * @see DATAMONGO-861
	 */
	@Test(expected = IllegalArgumentException.class)
	public void shouldRejectNullCondition() {
		new IfNullOperator(null, "");
	}

	/**
	 * @see DATAMONGO-861
	 */
	@Test(expected = IllegalArgumentException.class)
	public void shouldRejectThenValue() {
		new IfNullOperator(Fields.field("aa"), null);
	}

	/**
	 * @see DATAMONGO-861
	 */
	@Test
	public void simpleIfNullShouldRenderCorrectly() {

		IfNullOperator operator = IfNullOperator.newBuilder() //
				.ifNull("optional") //
				.thenReplaceWith("a more sophisticated value");

		DBObject dbObject = operator.toDbObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(dbObject,
				isBsonObject().containing("$ifNull", Arrays.<Object> asList("$optional", "a more sophisticated value")));
	}

	/**
	 * @see DATAMONGO-861
	 */
	@Test
	public void fieldReplacementIfNullShouldRenderCorrectly() {

		IfNullOperator operator = IfNullOperator.newBuilder() //
				.ifNull(Fields.field("optional")) //
				.thenReplaceWith(Fields.field("never-null"));

		DBObject dbObject = operator.toDbObject(Aggregation.DEFAULT_CONTEXT);

		assertThat(dbObject, isBsonObject().containing("$ifNull", Arrays.<Object> asList("$optional", "$never-null")));
	}
}
