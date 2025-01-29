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

import org.bson.BinaryVector;
import org.bson.Float32BinaryVector;
import org.bson.Int8BinaryVector;
import org.bson.PackedBitBinaryVector;

import org.springframework.data.domain.Vector;
import org.springframework.util.ObjectUtils;

/**
 * MongoDB-specific extension to {@link Vector} based on Mongo's {@link BinaryVector}. Note that only float32 and int8
 * variants can be represented as floating-point numbers. int1 returns an all-zero array for {@link #toFloatArray()} and
 * {@link #toDoubleArray()}.
 *
 * @author Mark Paluch
 * @since 4.5
 */
public class MongoVector implements Vector {

	private final BinaryVector v;

	MongoVector(BinaryVector v) {
		this.v = v;
	}

	/**
	 * Creates a new {@link MongoVector} from the given {@link BinaryVector}.
	 *
	 * @param v binary vector representation.
	 * @return the {@link MongoVector} for the given vector values.
	 */
	public static MongoVector of(BinaryVector v) {
		return new MongoVector(v);
	}

	@Override
	public Class<? extends Number> getType() {

		if (v instanceof Float32BinaryVector) {
			return Float.class;
		}

		if (v instanceof Int8BinaryVector) {
			return Byte.class;
		}

		if (v instanceof PackedBitBinaryVector) {
			return Byte.class;
		}

		return Number.class;
	}

	@Override
	public BinaryVector getSource() {
		return v;
	}

	@Override
	public int size() {

		if (v instanceof Float32BinaryVector f) {
			return f.getData().length;
		}

		if (v instanceof Int8BinaryVector i) {
			return i.getData().length;
		}

		if (v instanceof PackedBitBinaryVector p) {
			return p.getData().length;
		}

		return 0;
	}

	@Override
	public float[] toFloatArray() {

		if (v instanceof Float32BinaryVector f) {

			float[] result = new float[f.getData().length];
			System.arraycopy(f.getData(), 0, result, 0, result.length);
			return result;
		}

		if (v instanceof Int8BinaryVector i) {

			float[] result = new float[i.getData().length];
			System.arraycopy(i.getData(), 0, result, 0, result.length);
			return result;
		}

		return new float[size()];
	}

	@Override
	public double[] toDoubleArray() {

		if (v instanceof Float32BinaryVector f) {

			float[] data = f.getData();
			double[] result = new double[data.length];
			for (int i = 0; i < data.length; i++) {
				result[i] = data[i];
			}

			return result;
		}

		if (v instanceof Int8BinaryVector i) {

			double[] result = new double[i.getData().length];
			System.arraycopy(i.getData(), 0, result, 0, result.length);
			return result;
		}

		return new double[size()];
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof MongoVector that)) {
			return false;
		}
		return ObjectUtils.nullSafeEquals(v, that.v);
	}

	@Override
	public int hashCode() {
		return ObjectUtils.nullSafeHashCode(v);
	}

	@Override
	public String toString() {
		return "MV[" + v + "]";
	}
}
