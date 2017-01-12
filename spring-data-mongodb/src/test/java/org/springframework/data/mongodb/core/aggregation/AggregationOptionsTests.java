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

import org.junit.Before;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Unit tests for {@link AggregationOptions}.
 * 
 * @author Thomas Darimont
 * @since 1.6
 */
public class AggregationOptionsTests {

	AggregationOptions aggregationOptions;

	@Before
	public void setup() {
		aggregationOptions = newAggregationOptions().explain(true).cursor(new BasicDBObject("foo", 1)).allowDiskUse(true)
				.build();

	}

	@Test // DATAMONGO-960
	public void aggregationOptionsBuilderShouldSetOptionsAccordingly() {

		assertThat(aggregationOptions.isAllowDiskUse(), is(true));
		assertThat(aggregationOptions.isExplain(), is(true));
		assertThat(aggregationOptions.getCursor(), is((DBObject) new BasicDBObject("foo", 1)));
	}

	@Test // DATAMONGO-960
	public void aggregationOptionsToString() {
		assertThat(aggregationOptions.toString(),
				is("{ \"allowDiskUse\" : true , \"explain\" : true , \"cursor\" : { \"foo\" : 1}}"));
	}
}
