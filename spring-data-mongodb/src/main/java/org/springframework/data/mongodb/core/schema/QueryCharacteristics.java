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
package org.springframework.data.mongodb.core.schema;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.bson.BsonNull;
import org.bson.Document;
import org.springframework.data.domain.Range;
import org.springframework.data.domain.Range.Bound;
import org.springframework.lang.Nullable;

/**
 * Encapsulation of individual {@link QueryCharacteristic query characteristics} used to define queries that can be
 * executed when using queryable encryption.
 *
 * @author Christoph Strobl
 * @since 4.5
 */
public class QueryCharacteristics implements Iterable<QueryCharacteristic> {

	/**
	 * instance indicating none
	 */
	private static final QueryCharacteristics NONE = new QueryCharacteristics(Collections.emptyList());

	private final List<QueryCharacteristic> characteristics;

	QueryCharacteristics(List<QueryCharacteristic> characteristics) {
		this.characteristics = characteristics;
	}

	/**
	 * @return marker instance indicating no characteristics have been defined.
	 */
	public static QueryCharacteristics none() {
		return NONE;
	}

	/**
	 * Create new {@link QueryCharacteristics} from given list of {@link QueryCharacteristic characteristics}.
	 *
	 * @param characteristics must not be {@literal null}.
	 * @return new instance of {@link QueryCharacteristics}.
	 */
	public static QueryCharacteristics of(List<QueryCharacteristic> characteristics) {
		return new QueryCharacteristics(List.copyOf(characteristics));
	}

	/**
	 * Create new {@link QueryCharacteristics} from given {@link QueryCharacteristic characteristics}.
	 *
	 * @param characteristics must not be {@literal null}.
	 * @return new instance of {@link QueryCharacteristics}.
	 */
	public static QueryCharacteristics of(QueryCharacteristic... characteristics) {
		return new QueryCharacteristics(Arrays.asList(characteristics));
	}

	/**
	 * @return the list of {@link QueryCharacteristic characteristics}.
	 */
	public List<QueryCharacteristic> getCharacteristics() {
		return characteristics;
	}

	@Override
	public Iterator<QueryCharacteristic> iterator() {
		return this.characteristics.iterator();
	}

	/**
	 * Create a new {@link RangeQuery range query characteristic} used to define range queries against an encrypted field.
	 *
	 * @param <T> targeted field type
	 * @return new instance of {@link RangeQuery}.
	 */
	public static <T> RangeQuery<T> range() {
		return new RangeQuery<>();
	}

	/**
	 * Create a new {@link EqualityQuery equality query characteristic} used to define equality queries against an
	 * encrypted field.
	 *
	 * @param <T> targeted field type
	 * @return new instance of {@link EqualityQuery}.
	 */
	public static <T> EqualityQuery<T> equality() {
		return new EqualityQuery<>(null);
	}

	/**
	 * {@link QueryCharacteristic} for equality comparison.
	 *
	 * @param <T>
	 * @since 4.5
	 */
	public static class EqualityQuery<T> implements QueryCharacteristic {

		private final @Nullable Long contention;

		/**
		 * Create new instance of {@link EqualityQuery}.
		 *
		 * @param contention can be {@literal null}.
		 */
		public EqualityQuery(@Nullable Long contention) {
			this.contention = contention;
		}

		/**
		 * @param contention concurrent counter partition factor.
		 * @return new instance of {@link EqualityQuery}.
		 */
		public EqualityQuery<T> contention(long contention) {
			return new EqualityQuery<>(contention);
		}

		@Override
		public String queryType() {
			return "equality";
		}

		@Override
		public Document toDocument() {
			return QueryCharacteristic.super.toDocument().append("contention", contention);
		}
	}

	/**
	 * {@link QueryCharacteristic} for range comparison.
	 *
	 * @param <T>
	 * @since 4.5
	 */
	public static class RangeQuery<T> implements QueryCharacteristic {

		private final @Nullable Range<T> valueRange;
		private final @Nullable Integer trimFactor;
		private final @Nullable Long sparsity;
		private final @Nullable Long precision;
		private final @Nullable Long contention;

		private RangeQuery() {
			this(Range.unbounded(), null, null, null, null);
		}

		/**
		 * Create new instance of {@link RangeQuery}.
		 *
		 * @param valueRange
		 * @param trimFactor
		 * @param sparsity
		 * @param contention
		 */
		public RangeQuery(@Nullable Range<T> valueRange, @Nullable Integer trimFactor, @Nullable Long sparsity,
				@Nullable Long precision, @Nullable Long contention) {
			this.valueRange = valueRange;
			this.trimFactor = trimFactor;
			this.sparsity = sparsity;
			this.precision = precision;
			this.contention = contention;
		}

		/**
		 * @param lower the lower value range boundary for the queryable field.
		 * @return new instance of {@link RangeQuery}.
		 */
		public RangeQuery<T> min(T lower) {

			Range<T> range = Range.of(Bound.inclusive(lower),
					valueRange != null ? valueRange.getUpperBound() : Bound.unbounded());
			return new RangeQuery<>(range, trimFactor, sparsity, precision, contention);
		}

		/**
		 * @param upper the upper value range boundary for the queryable field.
		 * @return new instance of {@link RangeQuery}.
		 */
		public RangeQuery<T> max(T upper) {

			Range<T> range = Range.of(valueRange != null ? valueRange.getLowerBound() : Bound.unbounded(),
					Bound.inclusive(upper));
			return new RangeQuery<>(range, trimFactor, sparsity, precision, contention);
		}

		/**
		 * @param trimFactor value to control the throughput of concurrent inserts and updates.
		 * @return new instance of {@link RangeQuery}.
		 */
		public RangeQuery<T> trimFactor(int trimFactor) {
			return new RangeQuery<>(valueRange, trimFactor, sparsity, precision, contention);
		}

		/**
		 * @param sparsity value to control the value density within the index.
		 * @return new instance of {@link RangeQuery}.
		 */
		public RangeQuery<T> sparsity(long sparsity) {
			return new RangeQuery<>(valueRange, trimFactor, sparsity, precision, contention);
		}

		/**
		 * @param contention concurrent counter partition factor.
		 * @return new instance of {@link RangeQuery}.
		 */
		public RangeQuery<T> contention(long contention) {
			return new RangeQuery<>(valueRange, trimFactor, sparsity, precision, contention);
		}

		/**
		 * @param precision digits considered comparing floating point numbers.
		 * @return new instance of {@link RangeQuery}.
		 */
		public RangeQuery<T> precision(long precision) {
			return new RangeQuery<>(valueRange, trimFactor, sparsity, precision, contention);
		}

		@Override
		public String queryType() {
			return "range";
		}

		@Override
		@SuppressWarnings("unchecked")
		public Document toDocument() {

			Document target = QueryCharacteristic.super.toDocument();
			if (contention != null) {
				target.append("contention", contention);
			}
			if (trimFactor != null) {
				target.append("trimFactor", trimFactor);
			}
			if (valueRange != null) {
				target.append("min", valueRange.getLowerBound().getValue().orElse((T) BsonNull.VALUE)).append("max",
						valueRange.getUpperBound().getValue().orElse((T) BsonNull.VALUE));
			}
			if (sparsity != null) {
				target.append("sparsity", sparsity);
			}

			return target;
		}
	}
}
