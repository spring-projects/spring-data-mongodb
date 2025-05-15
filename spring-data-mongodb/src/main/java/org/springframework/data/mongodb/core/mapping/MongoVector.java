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
 * MongoDB-specific extension to {@link Vector} based on Mongo's {@link BinaryVector}. Note that only {@code float32}
 * and {@code int8} variants can be represented as floating-point numbers. {@code int1} throws
 * {@link UnsupportedOperationException} when calling {@link #toFloatArray()} and {@link #toDoubleArray()}.
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
	 * Creates a new binary {@link MongoVector} using the given {@link BinaryVector}.
	 *
	 * @param v binary vector representation.
	 * @return the {@link MongoVector} wrapping {@link BinaryVector}.
	 */
	public static MongoVector of(BinaryVector v) {
		return new MongoVector(v);
	}

	/**
	 * Creates a new binary {@link MongoVector} using the given {@code data}.
	 * <p>
	 * A {@link BinaryVector.DataType#INT8} vector is a vector of 8-bit signed integers where each byte in the vector
	 * represents an element of a vector, with values in the range {@code [-128, 127]}.
	 * <p>
	 * NOTE: The byte array is not copied; changes to the provided array will be referenced in the created
	 * {@code MongoVector} instance.
	 *
	 * @param data the byte array representing the {@link BinaryVector.DataType#INT8} vector data.
	 * @return the {@link MongoVector} containing the given vector values to be represented as binary {@code int8}.
	 */
	public static MongoVector ofInt8(byte[] data) {
		return of(BinaryVector.int8Vector(data));
	}

	/**
	 * Creates a new binary {@link MongoVector} using the given {@code data}.
	 * <p>
	 * A {@link BinaryVector.DataType#FLOAT32} vector is a vector of floating-point numbers, where each element in the
	 * vector is a {@code float}.
	 * <p>
	 * NOTE: The float array is not copied; changes to the provided array will be referenced in the created
	 * {@code MongoVector} instance.
	 *
	 * @param data the float array representing the {@link BinaryVector.DataType#FLOAT32} vector data.
	 * @return the {@link MongoVector} containing the given vector values to be represented as binary {@code float32}.
	 */
	public static MongoVector ofFloat(float... data) {
		return of(BinaryVector.floatVector(data));
	}

	/**
	 * Creates a new binary {@link MongoVector} from the given {@link Vector}.
	 * <p>
	 * A {@link BinaryVector.DataType#FLOAT32} vector is a vector of floating-point numbers, where each element in the
	 * vector is a {@code float}. The given {@link Vector} must be able to return a {@link Vector#toFloatArray() float}
	 * array.
	 * <p>
	 * NOTE: The float array is not copied; changes to the provided array will be referenced in the created
	 * {@code MongoVector} instance.
	 *
	 * @param v the
	 * @return the {@link MongoVector} using vector values from the given {@link Vector} to be represented as binary
	 *         float32.
	 */
	public static MongoVector fromFloat(Vector v) {
		return of(BinaryVector.floatVector(v.toFloatArray()));
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

	/**
	 * {@inheritDoc}
	 *
	 * @throws UnsupportedOperationException if the underlying data type is {@code int1} {@link PackedBitBinaryVector}.
	 */
	@Override
	public float[] toFloatArray() {

		if (v instanceof Float32BinaryVector f) {

			float[] result = new float[f.getData().length];
			System.arraycopy(f.getData(), 0, result, 0, result.length);
			return result;
		}

		if (v instanceof Int8BinaryVector i) {

			byte[] data = i.getData();
			float[] result = new float[data.length];
			for (int j = 0; j < data.length; j++) {
				result[j] = data[j];
			}
			return result;
		}

		throw new UnsupportedOperationException("Cannot return float array for " + v.getClass());
	}

	/**
	 * {@inheritDoc}
	 *
	 * @throws UnsupportedOperationException if the underlying data type is {@code int1} {@link PackedBitBinaryVector}.
	 */
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

			byte[] data = i.getData();
			double[] result = new double[data.length];
			for (int j = 0; j < data.length; j++) {
				result[j] = data[j];
			}
			return result;
		}

		throw new UnsupportedOperationException("Cannot return double array for " + v.getClass());
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
