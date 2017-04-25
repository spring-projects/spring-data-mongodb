/*
 * Copyright 2014-2017 the original author or authors.
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
import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;

import org.bson.Document;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link AggregationOptions}.
 * 
 * @author Thomas Darimont
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 1.6
 */
public class AggregationOptionsTests {

	AggregationOptions aggregationOptions;

	@Before
	public void setup() {
		aggregationOptions = newAggregationOptions().explain(true) //
				.cursorBatchSize(1) //
				.allowDiskUse(true) //
				.build();
	}

	@Test // DATAMONGO-960
	public void aggregationOptionsBuilderShouldSetOptionsAccordingly() {

		assertThat(aggregationOptions.isAllowDiskUse(), is(true));
		assertThat(aggregationOptions.isExplain(), is(true));
		assertThat(aggregationOptions.getCursor().get(), is(new Document("batchSize", 1)));
	}

	@Test // DATAMONGO-1637
	public void shouldInitializeFromDocument() {

		Document document = new Document();
		document.put("cursor", new Document("batchSize", 1));
		document.put("explain", true);
		document.put("allowDiskUse", true);

		aggregationOptions = AggregationOptions.fromDocument(document);

		assertThat(aggregationOptions.isAllowDiskUse(), is(true));
		assertThat(aggregationOptions.isExplain(), is(true));
		assertThat(aggregationOptions.getCursor().get(), is(new Document("batchSize", 1)));
		assertThat(aggregationOptions.getCursorBatchSize(), is(1));
	}

	@Test // DATAMONGO-960
	public void aggregationOptionsToString() {
		assertThat(aggregationOptions.toDocument(),
				is(Document.parse("{ \"allowDiskUse\" : true , \"explain\" : true , \"cursor\" : { \"batchSize\" : 1}}")));
	}
}
