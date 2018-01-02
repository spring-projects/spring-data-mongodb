/*
 * Copyright 2018-2018 the original author or authors.
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

import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;
import static org.springframework.data.mongodb.test.util.Assertions.*;

import java.util.Arrays;

import org.bson.Document;
import org.junit.Test;

/**
 * Unit tests for {@link SortByCountOperation}.
 *
 * @author Mark Paluch
 */
public class SortByCountOperationUnitTests {

	@Test // DATAMONGO-1553
	public void shouldRenderFieldCorrectly() {

		SortByCountOperation operation = sortByCount("country");
		Document result = operation.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(result).containsEntry("$sortByCount", "$country");
	}

	@Test // DATAMONGO-1553
	public void shouldRenderExpressionCorrectly() {

		SortByCountOperation operation = sortByCount(StringOperators.valueOf("foo").substring(5));
		Document result = operation.toDocument(Aggregation.DEFAULT_CONTEXT);

		assertThat(result).containsEntry("$sortByCount.$substr", Arrays.asList("$foo", 5, -1));
	}
}
