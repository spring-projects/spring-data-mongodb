/*
 * Copyright 2018-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.query;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import java.util.Arrays;

import org.bson.types.Binary;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.test.util.MongoTemplateExtension;
import org.springframework.data.mongodb.test.util.MongoTestTemplate;
import org.springframework.data.mongodb.test.util.Template;
import org.springframework.util.Base64Utils;

/**
 * Integration tests for {@link Criteria} usage as part of a {@link Query}.
 *
 * @author Christoph Strobl
 * @author Andreas Zink
 */
@ExtendWith(MongoTemplateExtension.class)
class CriteriaTests {

	@Template(initialEntitySet = { DocumentWithBitmask.class }) //
	static MongoTestTemplate ops;

	static final DocumentWithBitmask FIFTY_FOUR/*00110110*/ = new DocumentWithBitmask("1", Integer.valueOf(54),
			Integer.toBinaryString(54));
	static final DocumentWithBitmask TWENTY_INT/*00010100*/ = new DocumentWithBitmask("2", Integer.valueOf(20),
			Integer.toBinaryString(20));
	static final DocumentWithBitmask TWENTY_FLOAT/*00010100*/ = new DocumentWithBitmask("3", Float.valueOf(20),
			Integer.toBinaryString(20));
	static final DocumentWithBitmask ONE_HUNDRED_TWO/*01100110*/ = new DocumentWithBitmask("4",
			new Binary(Base64Utils.decodeFromString("Zg==")), "01100110");

	@BeforeEach
	void beforeEach() {

		ops.flush();

		ops.insert(FIFTY_FOUR);
		ops.insert(TWENTY_INT);
		ops.insert(TWENTY_FLOAT);
		ops.insert(ONE_HUNDRED_TWO);
	}

	@Test // DATAMONGO-1808
	public void bitsAllClearWithBitPositions() {

		assertThat(ops.find(query(where("value").bits().allClear(Arrays.asList(1, 5))), DocumentWithBitmask.class))
				.containsExactlyInAnyOrder(TWENTY_INT, TWENTY_FLOAT);
	}

	@Test // DATAMONGO-1808
	public void bitsAllClearWithNumericBitmask() {

		assertThat(ops.find(query(where("value").bits().allClear(35)), DocumentWithBitmask.class))
				.containsExactlyInAnyOrder(TWENTY_INT, TWENTY_FLOAT);
	}

	@Test // DATAMONGO-1808
	public void bitsAllClearWithStringBitmask() {

		assertThat(ops.find(query(where("value").bits().allClear("ID==")), DocumentWithBitmask.class))
				.containsExactlyInAnyOrder(TWENTY_INT, TWENTY_FLOAT);
	}

	@Test // DATAMONGO-1808
	public void bitsAllSetWithBitPositions() {

		assertThat(ops.find(query(where("value").bits().allSet(Arrays.asList(1, 5))), DocumentWithBitmask.class))
				.containsExactlyInAnyOrder(FIFTY_FOUR, ONE_HUNDRED_TWO);
	}

	@Test // DATAMONGO-1808
	public void bitsAllSetWithNumericBitmask() {

		assertThat(ops.find(query(where("value").bits().allSet(50)), DocumentWithBitmask.class))
				.containsExactlyInAnyOrder(FIFTY_FOUR);
	}

	@Test // DATAMONGO-1808
	public void bitsAllSetWithStringBitmask() {

		assertThat(ops.find(query(where("value").bits().allSet("MC==")), DocumentWithBitmask.class))
				.containsExactlyInAnyOrder(FIFTY_FOUR);
	}

	@Test // DATAMONGO-1808
	public void bitsAnyClearWithBitPositions() {

		assertThat(ops.find(query(where("value").bits().anyClear(Arrays.asList(1, 5))), DocumentWithBitmask.class))
				.containsExactlyInAnyOrder(TWENTY_INT, TWENTY_FLOAT);
	}

	@Test // DATAMONGO-1808
	public void bitsAnyClearWithNumericBitmask() {

		assertThat(ops.find(query(where("value").bits().anyClear(35)), DocumentWithBitmask.class))
				.containsExactlyInAnyOrder(FIFTY_FOUR, TWENTY_INT, TWENTY_FLOAT, ONE_HUNDRED_TWO);
	}

	@Test // DATAMONGO-1808
	public void bitsAnyClearWithStringBitmask() {

		assertThat(ops.find(query(where("value").bits().anyClear("MC==")), DocumentWithBitmask.class))
				.containsExactlyInAnyOrder(TWENTY_INT, TWENTY_FLOAT, ONE_HUNDRED_TWO);
	}

	@Test // DATAMONGO-1808
	public void bitsAnySetWithBitPositions() {

		assertThat(ops.find(query(where("value").bits().anySet(Arrays.asList(1, 5))), DocumentWithBitmask.class))
				.containsExactlyInAnyOrder(FIFTY_FOUR, ONE_HUNDRED_TWO);
	}

	@Test // DATAMONGO-1808
	public void bitsAnySetWithNumericBitmask() {

		assertThat(ops.find(query(where("value").bits().anySet(35)), DocumentWithBitmask.class))
				.containsExactlyInAnyOrder(FIFTY_FOUR, ONE_HUNDRED_TWO);
	}

	@Test // DATAMONGO-1808
	public void bitsAnySetWithStringBitmask() {

		assertThat(ops.find(query(where("value").bits().anySet("MC==")), DocumentWithBitmask.class))
				.containsExactlyInAnyOrder(FIFTY_FOUR, TWENTY_INT, TWENTY_FLOAT, ONE_HUNDRED_TWO);
	}

	static class DocumentWithBitmask {

		@Id String id;
		Object value;
		String binaryValue;

		public DocumentWithBitmask(String id, Object value, String binaryValue) {

			this.id = id;
			this.value = value;
			this.binaryValue = binaryValue;
		}

		public String getId() {
			return this.id;
		}

		public Object getValue() {
			return this.value;
		}

		public String getBinaryValue() {
			return this.binaryValue;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setValue(Object value) {
			this.value = value;
		}

		public void setBinaryValue(String binaryValue) {
			this.binaryValue = binaryValue;
		}

		public String toString() {
			return "CriteriaTests.DocumentWithBitmask(id=" + this.getId() + ", value=" + this.getValue() + ", binaryValue="
					+ this.getBinaryValue() + ")";
		}

		public boolean equals(final Object o) {
			if (o == this)
				return true;
			if (!(o instanceof DocumentWithBitmask))
				return false;
			final DocumentWithBitmask other = (DocumentWithBitmask) o;
			if (!other.canEqual((Object) this))
				return false;
			final Object this$id = this.getId();
			final Object other$id = other.getId();
			if (this$id == null ? other$id != null : !this$id.equals(other$id))
				return false;
			final Object this$binaryValue = this.getBinaryValue();
			final Object other$binaryValue = other.getBinaryValue();
			if (this$binaryValue == null ? other$binaryValue != null : !this$binaryValue.equals(other$binaryValue))
				return false;
			return true;
		}

		protected boolean canEqual(final Object other) {
			return other instanceof DocumentWithBitmask;
		}

		public int hashCode() {
			final int PRIME = 59;
			int result = 1;
			final Object $id = this.getId();
			result = result * PRIME + ($id == null ? 43 : $id.hashCode());
			final Object $binaryValue = this.getBinaryValue();
			result = result * PRIME + ($binaryValue == null ? 43 : $binaryValue.hashCode());
			return result;
		}
	}
}
