/*
 * Copyright 2025. the original author or authors.
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

/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.mongodb.core.schema;

import java.util.ArrayList;
import java.util.List;

import org.bson.BsonNull;
import org.bson.Document;
import org.springframework.data.domain.Range;
import org.springframework.data.domain.Range.Bound;
import org.springframework.lang.Nullable;

/**
 * @author Christoph Strobl
 */
public class QueryCharacteristics {

	private static final QueryCharacteristics NONE = new QueryCharacteristics(List.of());

	private final List<QueryCharacteristic> characteristics;

	public QueryCharacteristics(List<QueryCharacteristic> characteristics) {
		this.characteristics = characteristics;
	}

	public static QueryCharacteristics none() {
		return NONE;
	}

	QueryCharacteristics(QueryCharacteristic... characteristics) {

		this.characteristics = new ArrayList<>(characteristics.length);
		for (QueryCharacteristic characteristic : characteristics) {
			addQuery(characteristic);
		}
	}

	public void addQuery(QueryCharacteristic characteristic) {
		this.characteristics.add(characteristic);
	}

	List<QueryCharacteristic> getCharacteristics() {
		return characteristics;
	}

	public static <T> RangeQuery<T> range() {
		return new RangeQuery<>();
	}

	public interface QueryCharacteristic {

		String type();

		default Document toDocument() {
			return new Document("queryType", type());
		}
	}

	public static class RangeQuery<T> implements QueryCharacteristic {

		private final @Nullable Range<T> valueRange;
		private final @Nullable Integer trimFactor;
		private final @Nullable Long sparsity;
		private final @Nullable Long contention;

		private RangeQuery() {
			this(Range.unbounded(), null, null, null);
		}

		public RangeQuery(Range<T> valueRange, Integer trimFactor, Long sparsity, Long contention) {
			this.valueRange = valueRange;
			this.trimFactor = trimFactor;
			this.sparsity = sparsity;
			this.contention = contention;
		}

		@Override
		public String type() {
			return "range";
		}

		public RangeQuery<T> min(T lower) {

			Range<T> range = Range.of(Bound.inclusive(lower),
					valueRange != null ? valueRange.getUpperBound() : Bound.unbounded());
			return new RangeQuery<>(range, trimFactor, sparsity, contention);
		}

		public RangeQuery<T> max(T upper) {

			Range<T> range = Range.of(valueRange != null ? valueRange.getLowerBound() : Bound.unbounded(),
					Bound.inclusive(upper));
			return new RangeQuery<>(range, trimFactor, sparsity, contention);
		}

		public RangeQuery<T> trimFactor(int trimFactor) {
			return new RangeQuery<>(valueRange, trimFactor, sparsity, contention);
		}

		public RangeQuery<T> sparsity(long sparsity) {
			return new RangeQuery<>(valueRange, trimFactor, sparsity, contention);
		}

		public RangeQuery<T> contention(long contention) {
			return new RangeQuery<>(valueRange, trimFactor, sparsity, contention);
		}

		@Override
		public Document toDocument() {

			return QueryCharacteristic.super.toDocument().append("contention", contention).append("trimFactor", trimFactor)
					.append("sparsity", sparsity).append("min", valueRange.getLowerBound().getValue().orElse((T)BsonNull.VALUE))
					.append("max", valueRange.getUpperBound().getValue().orElse((T)BsonNull.VALUE));
		}
	}

}
