/*
 * Copyright 2022-2023 the original author or authors.
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

import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.bson.Document;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/***
 * Encapsulates the aggregation framework {@code $densify}-operation.
 *
 * @author Christoph Strobl
 * @since 4.0
 */
public class DensifyOperation implements AggregationOperation {

	private @Nullable Field field;
	private @Nullable List<?> partitionBy;
	private @Nullable Range range;

	protected DensifyOperation(@Nullable Field field, @Nullable List<?> partitionBy, @Nullable Range range) {

		this.field = field;
		this.partitionBy = partitionBy;
		this.range = range;
	}

	/**
	 * Obtain a builder to create the {@link DensifyOperation}.
	 *
	 * @return new instance of {@link DensifyOperationBuilder}.
	 */
	public static DensifyOperationBuilder builder() {
		return new DensifyOperationBuilder();
	}

	@Override
	public Document toDocument(AggregationOperationContext context) {

		Document densify = new Document();
		densify.put("field", context.getReference(field).getRaw());
		if (!ObjectUtils.isEmpty(partitionBy)) {
			densify.put("partitionByFields", partitionBy.stream().map(it -> {
				if (it instanceof Field field) {
					return context.getReference(field).getRaw();
				}
				if (it instanceof AggregationExpression expression) {
					return expression.toDocument(context);
				}
				return it;
			}).collect(Collectors.toList()));
		}
		densify.put("range", range.toDocument(context));
		return new Document("$densify", densify);
	}

	/**
	 * The {@link Range} specifies how the data is densified.
	 */
	public interface Range {

		/**
		 * Add documents spanning the range of values within the given lower (inclusive) and upper (exclusive) bound.
		 *
		 * @param lower must not be {@literal null}.
		 * @param upper must not be {@literal null}.
		 * @return new instance of {@link DensifyRange}.
		 */
		static DensifyRange bounded(Object lower, Object upper) {
			return new BoundedRange(lower, upper, DensifyUnits.NONE);
		}

		/**
		 * Add documents spanning the full value range.
		 *
		 * @return new instance of {@link DensifyRange}.
		 */
		static DensifyRange full() {

			return new DensifyRange(DensifyUnits.NONE) {

				@Override
				Object getBounds(AggregationOperationContext ctx) {
					return "full";
				}
			};
		}

		/**
		 * Add documents spanning the full value range for each partition.
		 *
		 * @return new instance of {@link DensifyRange}.
		 */
		static DensifyRange partition() {
			return new DensifyRange(DensifyUnits.NONE) {

				@Override
				Object getBounds(AggregationOperationContext ctx) {
					return "partition";
				}
			};
		}

		/**
		 * Obtain the document representation of the window in a default {@link AggregationOperationContext context}.
		 *
		 * @return never {@literal null}.
		 */
		default Document toDocument() {
			return toDocument(Aggregation.DEFAULT_CONTEXT);
		}

		/**
		 * Obtain the document representation of the window in the given {@link AggregationOperationContext context}.
		 *
		 * @return never {@literal null}.
		 */
		Document toDocument(AggregationOperationContext ctx);
	}

	/**
	 * Base {@link Range} implementation.
	 *
	 * @author Christoph Strobl
	 */
	public static abstract class DensifyRange implements Range {

		private @Nullable DensifyUnit unit;
		private Number step;

		public DensifyRange(DensifyUnit unit) {
			this.unit = unit;
		}

		@Override
		public Document toDocument(AggregationOperationContext ctx) {

			Document range = new Document("step", step);
			if (unit != null && !DensifyUnits.NONE.equals(unit)) {
				range.put("unit", unit.name().toLowerCase(Locale.US));
			}
			range.put("bounds", getBounds(ctx));
			return range;
		}

		/**
		 * Set the increment for the value.
		 *
		 * @param step must not be {@literal null}.
		 * @return this.
		 */
		public DensifyRange incrementBy(Number step) {
			this.step = step;
			return this;
		}

		/**
		 * Set the increment for the value.
		 *
		 * @param step must not be {@literal null}.
		 * @return this.
		 */
		public DensifyRange incrementBy(Number step, DensifyUnit unit) {
			this.step = step;
			return unit(unit);
		}

		/**
		 * Set the {@link DensifyUnit unit} for the step field.
		 *
		 * @param unit
		 * @return this.
		 */
		public DensifyRange unit(DensifyUnit unit) {

			this.unit = unit;
			return this;
		}

		abstract Object getBounds(AggregationOperationContext ctx);
	}

	/**
	 * {@link Range} implementation holding lower and upper bound values.
	 *
	 * @author Christoph Strobl
	 */
	public static class BoundedRange extends DensifyRange {

		private List<Object> bounds;

		protected BoundedRange(Object lower, Object upper, DensifyUnit unit) {

			super(unit);
			this.bounds = Arrays.asList(lower, upper);
		}

		@Override
		List<Object> getBounds(AggregationOperationContext ctx) {
			return bounds.stream().map(it -> {
				if (it instanceof AggregationExpression expression) {
					return expression.toDocument(ctx);
				}
				return it;
			}).collect(Collectors.toList());
		}
	}

	/**
	 * The actual time unit to apply to a {@link Range}.
	 */
	public interface DensifyUnit {

		String name();

		/**
		 * Converts the given time unit into a {@link DensifyUnit}. Supported units are: days, hours, minutes, seconds, and
		 * milliseconds.
		 *
		 * @param timeUnit the time unit to convert, must not be {@literal null}.
		 * @return
		 * @throws IllegalArgumentException if the {@link TimeUnit} is {@literal null} or not supported for conversion.
		 */
		static DensifyUnit from(TimeUnit timeUnit) {

			Assert.notNull(timeUnit, "TimeUnit must not be null");

			switch (timeUnit) {
				case DAYS:
					return DensifyUnits.DAY;
				case HOURS:
					return DensifyUnits.HOUR;
				case MINUTES:
					return DensifyUnits.MINUTE;
				case SECONDS:
					return DensifyUnits.SECOND;
				case MILLISECONDS:
					return DensifyUnits.MILLISECOND;
			}

			throw new IllegalArgumentException(String.format("Cannot create DensifyUnit from %s", timeUnit));
		}

		/**
		 * Converts the given chrono unit into a {@link DensifyUnit}. Supported units are: years, weeks, months, days,
		 * hours, minutes, seconds, and millis.
		 *
		 * @param chronoUnit the chrono unit to convert, must not be {@literal null}.
		 * @return
		 * @throws IllegalArgumentException if the {@link TimeUnit} is {@literal null} or not supported for conversion.
		 */
		static DensifyUnits from(ChronoUnit chronoUnit) {

			switch (chronoUnit) {
				case YEARS:
					return DensifyUnits.YEAR;
				case WEEKS:
					return DensifyUnits.WEEK;
				case MONTHS:
					return DensifyUnits.MONTH;
				case DAYS:
					return DensifyUnits.DAY;
				case HOURS:
					return DensifyUnits.HOUR;
				case MINUTES:
					return DensifyUnits.MINUTE;
				case SECONDS:
					return DensifyUnits.SECOND;
				case MILLIS:
					return DensifyUnits.MILLISECOND;
			}

			throw new IllegalArgumentException(String.format("Cannot create DensifyUnit from %s", chronoUnit));
		}
	}

	/**
	 * Quick access to available {@link DensifyUnit units}.
	 */
	public enum DensifyUnits implements DensifyUnit {
		NONE, YEAR, QUARTER, MONTH, WEEK, DAY, HOUR, MINUTE, SECOND, MILLISECOND
	}

	public static class DensifyOperationBuilder {

		DensifyOperation target;

		public DensifyOperationBuilder() {
			this.target = new DensifyOperation(null, Collections.emptyList(), null);
		}

		/**
		 * Set the field to densify.
		 *
		 * @param fieldname must not be {@literal null}.
		 * @return this.
		 */
		public DensifyOperationBuilder densify(String fieldname) {
			this.target.field = Fields.field(fieldname);
			return this;
		}

		/**
		 * Set the fields used for grouping documents.
		 *
		 * @param fields must not be {@literal null}.
		 * @return this.
		 */
		public DensifyOperationBuilder partitionBy(String... fields) {
			target.partitionBy = Fields.fields(fields).asList();
			return this;
		}

		/**
		 * Set the operational range.
		 *
		 * @param range must not be {@literal null}.
		 * @return this.
		 */
		public DensifyOperationBuilder range(Range range) {

			target.range = range;
			return this;
		}

		/**
		 * Operate on full range.
		 *
		 * @param consumer
		 * @return this.
		 */
		public DensifyOperationBuilder fullRange(Consumer<DensifyRange> consumer) {

			Assert.notNull(consumer, "Consumer must not be null");

			DensifyRange range = Range.full();
			consumer.accept(range);

			return range(range);
		}

		/**
		 * Operate on full range.
		 *
		 * @param consumer
		 * @return this.
		 */
		public DensifyOperationBuilder partitionRange(Consumer<DensifyRange> consumer) {

			DensifyRange range = Range.partition();
			consumer.accept(range);

			return range(range);
		}

		public DensifyOperation build() {
			return new DensifyOperation(target.field, target.partitionBy, target.range);
		}
	}
}
