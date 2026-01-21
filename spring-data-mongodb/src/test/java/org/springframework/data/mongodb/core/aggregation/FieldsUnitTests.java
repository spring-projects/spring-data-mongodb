/*
 * Copyright 2013-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.aggregation;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.mongodb.core.aggregation.Fields.*;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link Fields}.
 *
 * @author Oliver Gierke
 * @author Thomas Darimont
 */
class FieldsUnitTests {

	@Test
	void rejectsNullFieldVarArgs() {
		assertThatIllegalArgumentException().isThrownBy(() -> Fields.from((Field[]) null));
	}

	@Test
	void rejectsNullFieldNameVarArgs() {
		assertThatIllegalArgumentException().isThrownBy(() -> Fields.fields((String[]) null));
	}

	@Test
	void createsFieldFromNameOnly() {
		verify(Fields.field("foo"), "foo", null);
	}

	@Test
	void createsFieldFromNameAndTarget() {
		verify(Fields.field("foo", "bar"), "foo", "bar");
	}

	@Test
	void rejectsNullFieldName() {
		assertThatIllegalArgumentException().isThrownBy(() -> Fields.field(null));
	}

	@Test
	void rejectsNullFieldNameIfTargetGiven() {
		assertThatIllegalArgumentException().isThrownBy(() -> Fields.field(null, "foo"));
	}

	@Test
	void rejectsEmptyFieldName() {
		assertThatIllegalArgumentException().isThrownBy(() -> Fields.field(""));
	}

	@Test
	void createsFieldsFromFieldInstances() {

		AggregationField reference = new AggregationField("foo");
		Fields fields = Fields.from(reference);

		assertThat(fields).hasSize(1);
		assertThat(fields).contains(reference);
	}

	@Test
	void aliasesPathExpressionsIntoLeafForImplicits() {
		verify(Fields.field("foo.bar"), "bar", "foo.bar");
	}

	@Test
	void fieldsFactoryMethod() {

		Fields fields = fields("a", "b").and("c").and("d", "e");

		assertThat(fields).hasSize(4);

		verify(fields.getField("a"), "a", null);
		verify(fields.getField("b"), "b", null);
		verify(fields.getField("c"), "c", null);
		verify(fields.getField("d"), "d", "e");
	}

	@Test
	void rejectsAmbiguousFieldNames() {
		assertThatIllegalArgumentException().isThrownBy(() -> fields("b", "a.b"));
	}

	@Test // DATAMONGO-774
	void stripsLeadingDollarsFromName() {

		assertThat(Fields.field("$name").getName()).isEqualTo("name");
		assertThat(Fields.field("$$$$name").getName()).isEqualTo("name");
	}

	@Test // DATAMONGO-774
	void rejectsNameConsistingOfDollarOnly() {
		assertThatIllegalArgumentException().isThrownBy(() -> Fields.field("$"));
	}

	@Test // DATAMONGO-774
	void stripsLeadingDollarsFromTarget() {

		assertThat(Fields.field("$target").getTarget()).isEqualTo("target");
		assertThat(Fields.field("$$$$target").getTarget()).isEqualTo("target");
	}

	@Test // GH-4123
	void keepsRawMappingToDbRefId() {

		assertThat(Fields.field("$id").getName()).isEqualTo("id");
		assertThat(Fields.field("person.$id").getTarget()).isEqualTo("person.$id");
	}

	private static void verify(Field field, String name, String target) {

		assertThat(field).isNotNull();
		assertThat(field.getName()).isEqualTo(name);
		assertThat(field.getTarget()).isEqualTo(target != null ? target : name);
	}

}
