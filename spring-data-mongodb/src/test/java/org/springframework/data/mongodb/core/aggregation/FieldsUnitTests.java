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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.springframework.data.mongodb.core.aggregation.Fields.*;

import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.data.mongodb.core.aggregation.Fields.AggregationField;

/**
 * Unit tests for {@link Fields}.
 * 
 * @author Oliver Gierke
 * @author Thomas Darimont
 */
public class FieldsUnitTests {

	@Rule public ExpectedException exception = ExpectedException.none();

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullFieldVarArgs() {
		Fields.from((Field[]) null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullFieldNameVarArgs() {
		Fields.fields((String[]) null);
	}

	@Test
	public void createsFieldFromNameOnly() {
		verify(Fields.field("foo"), "foo", null);
	}

	@Test
	public void createsFieldFromNameAndTarget() {
		verify(Fields.field("foo", "bar"), "foo", "bar");
	}

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullFieldName() {
		Fields.field(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullFieldNameIfTargetGiven() {
		Fields.field(null, "foo");
	}

	@Test(expected = IllegalArgumentException.class)
	public void rejectsEmptyFieldName() {
		Fields.field("");
	}

	@Test
	public void createsFieldsFromFieldInstances() {

		AggregationField reference = new AggregationField("foo");
		Fields fields = Fields.from(reference);

		assertThat(fields, is(Matchers.<Field> iterableWithSize(1)));
		assertThat(fields, hasItem(reference));
	}

	@Test
	public void aliasesPathExpressionsIntoLeafForImplicits() {
		verify(Fields.field("foo.bar"), "bar", "foo.bar");
	}

	@Test
	public void fieldsFactoryMethod() {

		Fields fields = fields("a", "b").and("c").and("d", "e");

		assertThat(fields, is(Matchers.<Field> iterableWithSize(4)));

		verify(fields.getField("a"), "a", null);
		verify(fields.getField("b"), "b", null);
		verify(fields.getField("c"), "c", null);
		verify(fields.getField("d"), "d", "e");
	}

	@Test
	public void rejectsAmbiguousFieldNames() {

		exception.expect(IllegalArgumentException.class);

		fields("b", "a.b");
	}

	/**
	 * @see DATAMONGO-774
	 */
	@Test
	public void stripsLeadingDollarsFromName() {

		assertThat(Fields.field("$name").getName(), is("name"));
		assertThat(Fields.field("$$$$name").getName(), is("name"));
	}

	/**
	 * @see DATAMONGO-774
	 */
	@Test(expected = IllegalArgumentException.class)
	public void rejectsNameConsistingOfDollarOnly() {
		Fields.field("$");
	}

	/**
	 * @see DATAMONGO-774
	 */
	@Test
	public void stripsLeadingDollarsFromTarget() {

		assertThat(Fields.field("$target").getTarget(), is("target"));
		assertThat(Fields.field("$$$$target").getTarget(), is("target"));
	}

	private static void verify(Field field, String name, String target) {

		assertThat(field, is(notNullValue()));
		assertThat(field.getName(), is(name));
		assertThat(field.getTarget(), is(target != null ? target : name));
	}
}
