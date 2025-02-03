/*
 * Copyright 2012-2025 the original author or authors.
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
package org.springframework.data.mongodb.core.convert;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

import org.bson.BinaryVector;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Vector;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.MongoVector;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.test.util.MongoTemplateExtension;
import org.springframework.data.mongodb.test.util.MongoTestTemplate;
import org.springframework.data.mongodb.test.util.Template;
import org.springframework.util.ObjectUtils;

/**
 * Integration tests for {@link MongoConverters}.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@ExtendWith(MongoTemplateExtension.class)
public class MongoConvertersIntegrationTests {

	static final String COLLECTION = "converter-tests";

	@Template //
	static MongoTestTemplate template;

	@BeforeEach
	public void setUp() {
		template.flush(COLLECTION);
	}

	@Test // DATAMONGO-422
	public void writesUUIDBinaryCorrectly() {

		Wrapper wrapper = new Wrapper();
		wrapper.uuid = UUID.randomUUID();
		template.save(wrapper);

		assertThat(wrapper.id).isNotNull();

		Wrapper result = template.findOne(Query.query(Criteria.where("id").is(wrapper.id)), Wrapper.class);
		assertThat(result.uuid).isEqualTo(wrapper.uuid);
	}

	@Test // DATAMONGO-1802
	public void shouldConvertBinaryDataOnRead() {

		WithBinaryDataInArray wbd = new WithBinaryDataInArray();
		wbd.data = "calliope-mini".getBytes();

		template.save(wbd);

		assertThat(template.findOne(query(where("id").is(wbd.id)), WithBinaryDataInArray.class)).isEqualTo(wbd);
	}

	@Test // DATAMONGO-1802
	public void shouldConvertEmptyBinaryDataOnRead() {

		WithBinaryDataInArray wbd = new WithBinaryDataInArray();
		wbd.data = new byte[0];

		template.save(wbd);

		assertThat(template.findOne(query(where("id").is(wbd.id)), WithBinaryDataInArray.class)).isEqualTo(wbd);
	}

	@Test // DATAMONGO-1802
	public void shouldReadBinaryType() {

		WithBinaryDataType wbd = new WithBinaryDataType();
		wbd.data = new Binary("calliope-mini".getBytes());

		template.save(wbd);

		assertThat(template.findOne(query(where("id").is(wbd.id)), WithBinaryDataType.class)).isEqualTo(wbd);
	}

	@Test // GH-4706
	public void shouldReadAndWriteVectors() {

		WithVectors source = new WithVectors();
		source.vector = Vector.of(1.1, 2.2, 3.3);

		template.save(source);

		WithVectors loaded = template.findOne(query(where("id").is(source.id)), WithVectors.class);
		assertThat(loaded).isEqualTo(source);
	}

	@Test // GH-4706
	public void shouldReadAndWriteFloatVectors() {

		WithVectors source = new WithVectors();
		source.vector = Vector.of(1.1f, 2.2f, 3.3f);

		template.save(source);

		WithVectors loaded = template.findOne(query(where("id").is(source.id)), WithVectors.class);

		// top-level arrays are converted into doubles by MongoDB with all their conversion imprecisions
		assertThat(loaded.vector.getClass().getName()).contains("DoubleVector");
		assertThat(loaded.vector).isNotEqualTo(source.vector);
	}

	@Test // GH-4706
	public void shouldReadAndWriteBinFloat32Vectors() {

		WithVectors source = new WithVectors();
		source.binVector = BinaryVector.floatVector(new float[] { 1.1f, 2.2f, 3.3f });
		source.vector = MongoVector.of(source.binVector);

		template.save(source);

		WithVectors loaded = template.findOne(query(where("id").is(source.id)), WithVectors.class);

		assertThat(loaded.vector).isEqualTo(source.vector);
		assertThat(loaded.binVector).isEqualTo(source.binVector);
	}

	@Test // GH-4706
	public void shouldReadAndWriteBinInt8Vectors() {

		WithVectors source = new WithVectors();
		source.binVector = BinaryVector.int8Vector(new byte[] { 1, 2, 3 });
		source.vector = MongoVector.of(source.binVector);

		template.save(source);

		WithVectors loaded = template.findOne(query(where("id").is(source.id)), WithVectors.class);

		assertThat(loaded.vector).isEqualTo(source.vector);
		assertThat(loaded.binVector).isEqualTo(source.binVector);
	}

	@Test // GH-4706
	public void shouldReadAndWriteBinPackedVectors() {

		WithVectors source = new WithVectors();
		source.binVector = BinaryVector.packedBitVector(new byte[] { 1, 2, 3 }, (byte) 1);
		source.vector = MongoVector.of(source.binVector);

		template.save(source);

		WithVectors loaded = template.findOne(query(where("id").is(source.id)), WithVectors.class);

		assertThat(loaded.vector).isEqualTo(source.vector);
		assertThat(loaded.binVector).isEqualTo(source.binVector);
	}

	@Document(COLLECTION)
	static class Wrapper {

		String id;
		UUID uuid;
	}

	@Document(COLLECTION)
	static class WithVectors {

		ObjectId id;
		Vector vector;
		BinaryVector binVector;

		@Override
		public boolean equals(Object o) {
			if (!(o instanceof WithVectors that)) {
				return false;
			}
			if (!ObjectUtils.nullSafeEquals(id, that.id)) {
				return false;
			}
			if (!ObjectUtils.nullSafeEquals(vector, that.vector)) {
				return false;
			}
			return ObjectUtils.nullSafeEquals(binVector, that.binVector);
		}

		@Override
		public int hashCode() {
			return ObjectUtils.nullSafeHash(id, vector, binVector);
		}
	}

	@Document(COLLECTION)
	static class WithBinaryDataInArray {

		@Id String id;
		byte[] data;

		public String getId() {
			return this.id;
		}

		public byte[] getData() {
			return this.data;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setData(byte[] data) {
			this.data = data;
		}

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			WithBinaryDataInArray that = (WithBinaryDataInArray) o;
			return Objects.equals(id, that.id) && Arrays.equals(data, that.data);
		}

		@Override
		public int hashCode() {
			int result = Objects.hash(id);
			result = 31 * result + Arrays.hashCode(data);
			return result;
		}

		public String toString() {
			return "MongoConvertersIntegrationTests.WithBinaryDataInArray(id=" + this.getId() + ", data="
					+ java.util.Arrays.toString(this.getData()) + ")";
		}
	}

	@Document(COLLECTION)
	static class WithBinaryDataType {

		@Id String id;
		Binary data;

		public WithBinaryDataType() {}

		public String getId() {
			return this.id;
		}

		public Binary getData() {
			return this.data;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setData(Binary data) {
			this.data = data;
		}

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			WithBinaryDataType that = (WithBinaryDataType) o;
			return Objects.equals(id, that.id) && Objects.equals(data, that.data);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, data);
		}

		public String toString() {
			return "MongoConvertersIntegrationTests.WithBinaryDataType(id=" + this.getId() + ", data=" + this.getData() + ")";
		}
	}
}
