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

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.springframework.data.mongodb.test.util.IsBsonObject.*;

import org.junit.Test;
import org.springframework.data.mongodb.core.DBObjectTestUtils;

import com.mongodb.DBObject;

/**
 * Unit tests for {@link LookupOperation}.
 *
 * @author Alessio Fachechi
 * @author Christoph Strobl
 */
public class LookupOperationUnitTests {

	/**
	 * @see DATAMONGO-1326
	 */
	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullForFrom() {
		new LookupOperation(null, Fields.field("localField"), Fields.field("foreignField"), Fields.field("as"));
	}

	/**
	 * @see DATAMONGO-1326
	 */
	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullLocalFieldField() {
		new LookupOperation(Fields.field("from"), null, Fields.field("foreignField"), Fields.field("as"));
	}

	/**
	 * @see DATAMONGO-1326
	 */
	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullForeignField() {
		new LookupOperation(Fields.field("from"), Fields.field("localField"), null, Fields.field("as"));
	}

	/**
	 * @see DATAMONGO-1326
	 */
	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullForAs() {
		new LookupOperation(Fields.field("from"), Fields.field("localField"), Fields.field("foreignField"), null);
	}

	/**
	 * @see DATAMONGO-1326
	 */
	@Test
	public void lookupOperationWithValues() {

		LookupOperation lookupOperation = Aggregation.lookup("a", "b", "c", "d");

		DBObject lookupClause = extractDbObjectFromLookupOperation(lookupOperation);

		assertThat(lookupClause,
				isBsonObject().containing("from", "a") //
						.containing("localField", "b") //
						.containing("foreignField", "c") //
						.containing("as", "d"));
	}

	/**
	 * @see DATAMONGO-1326
	 */
	@Test
	public void lookupOperationExposesAsField() {

		LookupOperation lookupOperation = Aggregation.lookup("a", "b", "c", "d");

		assertThat(lookupOperation.getFields().exposesNoFields(), is(false));
		assertThat(lookupOperation.getFields().exposesSingleFieldOnly(), is(true));
		assertThat(lookupOperation.getFields().getField("d"), notNullValue());
	}

	private DBObject extractDbObjectFromLookupOperation(LookupOperation lookupOperation) {

		DBObject dbObject = lookupOperation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		DBObject lookupClause = DBObjectTestUtils.getAsDBObject(dbObject, "$lookup");
		return lookupClause;
	}
}
