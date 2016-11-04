/*
 * Copyright 2013-2016 the original author or authors.
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
import org.springframework.data.mongodb.core.DocumentTestUtils;
import org.springframework.data.mongodb.core.query.NearQuery;

/**
 * Unit tests for {@link GeoNearOperation}.
 * 
 * @author Oliver Gierke
 * @author Thomas Darimont
 */
public class GeoNearOperationUnitTests {

	/**
	 * @see DATAMONGO-1127
	 */
	@Test
	public void rendersNearQueryAsAggregationOperation() {

		NearQuery query = NearQuery.near(10.0, 10.0);
		GeoNearOperation operation = new GeoNearOperation(query, "distance");
		Document document = operation.toDocument(Aggregation.DEFAULT_CONTEXT);

		Document nearClause = DocumentTestUtils.getAsDocument(document, "$geoNear");

		Document expected = new Document(query.toDocument()).append("distanceField", "distance");
		assertThat(nearClause, is(expected));
	}
}
