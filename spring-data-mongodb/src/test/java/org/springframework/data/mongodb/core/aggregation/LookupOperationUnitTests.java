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

import com.mongodb.DBObject;
import org.junit.Test;
import org.springframework.data.mongodb.core.DBObjectTestUtils;

/**
 * Unit tests for {@link LookupOperation}.
 *
 * @author Alessio Fachechi
 */
public class LookupOperationUnitTests {

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullFields() {
		new LookupOperation((Field) null, (Field) null, (Field) null, (Field) null);
	}

	@Test
	public void lookupOperationWithValues() {

		LookupOperation lookupOperation = Aggregation.lookup("a", "b", "c", "d");

		DBObject lookupClause = extractDbObjectFromLookupOperation(lookupOperation);

		assertThat((String) lookupClause.get("from"), is(new String("a")));
		assertThat((String) lookupClause.get("localField"), is(new String("b")));
		assertThat((String) lookupClause.get("foreignField"), is(new String("c")));
		assertThat((String) lookupClause.get("as"), is(new String("d")));

		assertThat(lookupOperation.getFields().exposesNoFields(), is(false));
		assertThat(lookupOperation.getFields().exposesSingleFieldOnly(), is(true));
	}

	private DBObject extractDbObjectFromLookupOperation(LookupOperation lookupOperation) {
		DBObject dbObject = lookupOperation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		DBObject lookupClause = DBObjectTestUtils.getAsDBObject(dbObject, "$lookup");
		return lookupClause;
	}
}
