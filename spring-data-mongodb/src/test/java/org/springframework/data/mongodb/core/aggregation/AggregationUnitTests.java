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
import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;

import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.data.mongodb.core.DBObjectTestUtils;

import com.mongodb.DBObject;

/**
 * Unit tests for {@link Aggregation}.
 * 
 * @author Oliver Gierke
 * @author Thomas Darimont
 */
public class AggregationUnitTests {

	public @Rule ExpectedException exception = ExpectedException.none();

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullAggregationOperation() {
		newAggregation((AggregationOperation[]) null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullTypedAggregationOperation() {
		newAggregation(String.class, (AggregationOperation[]) null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNoAggregationOperation() {
		newAggregation(new AggregationOperation[0]);
	}

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNoTypedAggregationOperation() {
		newAggregation(String.class, new AggregationOperation[0]);
	}

	/**
	 * @see DATAMONGO-753
	 */
	@Test
	public void checkForCorrectFieldScopeTransfer() {

		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Invalid reference");
		exception.expectMessage("'b'");

		newAggregation( //
				project("a", "b"), //
				group("a").count().as("cnt"), // a was introduced to the context by the project operation
				project("cnt", "b") // b was removed from the context by the group operation
		).toDbObject("foo", Aggregation.DEFAULT_CONTEXT); // -> triggers IllegalArgumentException
	}

	/**
	 * @see DATAMONGO-753
	 */
	@Test
	public void unwindOperationShouldNotChangeAvailableFields() {

		newAggregation( //
				project("a", "b"), //
				unwind("a"), //
				project("a", "b") // b should still be available
		).toDbObject("foo", Aggregation.DEFAULT_CONTEXT);
	}

	/**
	 * @see DATAMONGO-753
	 */
	@Test
	public void matchOperationShouldNotChangeAvailableFields() {

		newAggregation( //
				project("a", "b"), //
				match(where("a").gte(1)), //
				project("a", "b") // b should still be available
		).toDbObject("foo", Aggregation.DEFAULT_CONTEXT);
	}

	/**
	 * @see DATAMONGO-788
	 */
	@Test
	public void referencesToGroupIdsShouldBeRenderedAsReferences() {

		DBObject agg = newAggregation( //
				project("a"), //
				group("a").count().as("aCnt"), //
				project("aCnt", "a") //
		).toDbObject("foo", Aggregation.DEFAULT_CONTEXT);

		@SuppressWarnings("unchecked")
		DBObject secondProjection = ((List<DBObject>) agg.get("pipeline")).get(2);
		DBObject fields = DBObjectTestUtils.getAsDBObject(secondProjection, "$project");
		assertThat((Integer) fields.get("aCnt"), is(1));
		assertThat((String) fields.get("a"), is("$_id.a"));
	}
}
