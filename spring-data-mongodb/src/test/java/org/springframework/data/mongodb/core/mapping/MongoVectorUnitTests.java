/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.mongodb.core.mapping;

import static org.springframework.data.mongodb.test.util.Assertions.*;

import org.bson.BinaryVector;
import org.bson.Float32BinaryVector;
import org.junit.jupiter.api.Test;

import org.springframework.data.domain.Vector;

/**
 * Unit tests for {@link MongoVector}.
 *
 * @author Mark Paluch
 */
class MongoVectorUnitTests {

	@Test // GH-4706
	void shouldReturnInt8AsFloatingPoints() {

		MongoVector vector = MongoVector.ofInt8(new byte[] { 1, 2, 3 });

		assertThat(vector.toDoubleArray()).contains(1, 2, 3);
		assertThat(vector.toFloatArray()).contains(1, 2, 3);
	}

	@Test // GH-4706
	void shouldReturnFloatAsFloatingPoints() {

		MongoVector vector = MongoVector.ofFloat(1f, 2f, 3f);

		assertThat(vector.toDoubleArray()).contains(1, 2, 3);
		assertThat(vector.toFloatArray()).contains(1, 2, 3);
	}

	@Test // GH-4706
	void ofFloatIsNotEqualToVectorOf() {

		MongoVector mv = MongoVector.ofFloat(1f, 2f, 3f);
		Vector v = Vector.of(1f, 2f, 3f);

		assertThat(v).isNotEqualTo(mv);
	}

	@Test // GH-4706
	void mongoVectorCanAdaptToFloatVector() {

		Vector v = Vector.of(1f, 2f, 3f);
		MongoVector mv = MongoVector.fromFloat(v);

		assertThat(mv.toFloatArray()).isEqualTo(v.toFloatArray());
		assertThat(mv.getSource()).isInstanceOf(Float32BinaryVector.class);
	}

	@Test // GH-4706
	void shouldNotReturnFloatsForPackedBit() {

		MongoVector vector = MongoVector.of(BinaryVector.packedBitVector(new byte[] { 1, 2, 3 }, (byte) 1));

		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(vector::toFloatArray);
		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(vector::toDoubleArray);
	}

}
