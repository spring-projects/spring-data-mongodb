/*
 * Copyright 2016. the original author or authors.
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

import java.util.LinkedHashMap;

import org.springframework.data.mongodb.core.aggregation.ArithmeticOperators.ArithmeticOperatorFactory;
import org.springframework.util.Assert;

/**
 * Gateway to {@literal Date} aggregation operations.
 *
 * @author Christoph Strobl
 * @since 1.10
 */
public class DateOperators {

	/**
	 * Take the date referenced by given {@literal fieldReference}.
	 *
	 * @param fieldReference must not be {@literal null}.
	 * @return
	 */
	public static DateOperatorFactory dateOf(String fieldReference) {

		Assert.notNull(fieldReference, "FieldReference must not be null!");
		return new DateOperatorFactory(fieldReference);
	}

	/**
	 * Take the date resulting from the given {@link AggregationExpression}.
	 *
	 * @param expression must not be {@literal null}.
	 * @return
	 */
	public static DateOperatorFactory dateOf(AggregationExpression expression) {

		Assert.notNull(expression, "Expression must not be null!");
		return new DateOperatorFactory(expression);
	}

	/**
	 * @author Christoph Strobl
	 */
	public static class DateOperatorFactory {

		private final String fieldReference;
		private final AggregationExpression expression;

		/**
		 * Creates new {@link ArithmeticOperatorFactory} for given {@literal fieldReference}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 */
		public DateOperatorFactory(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			this.fieldReference = fieldReference;
			this.expression = null;
		}

		/**
		 * Creates new {@link ArithmeticOperatorFactory} for given {@link AggregationExpression}.
		 *
		 * @param expression must not be {@literal null}.
		 */
		public DateOperatorFactory(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			this.fieldReference = null;
			this.expression = expression;
		}

		/**
		 * Creates new {@link AggregationExpressions} that returns the day of the year for a date as a number between 1 and
		 * 366.
		 *
		 * @return
		 */
		public DayOfYear dayOfYear() {
			return usesFieldRef() ? DayOfYear.dayOfYear(fieldReference) : DayOfYear.dayOfYear(expression);
		}

		/**
		 * Creates new {@link AggregationExpressions} that returns the day of the month for a date as a number between 1 and
		 * 31.
		 *
		 * @return
		 */
		public DayOfMonth dayOfMonth() {
			return usesFieldRef() ? DayOfMonth.dayOfMonth(fieldReference) : DayOfMonth.dayOfMonth(expression);
		}

		/**
		 * Creates new {@link AggregationExpressions} that returns the day of the week for a date as a number between 1
		 * (Sunday) and 7 (Saturday).
		 *
		 * @return
		 */
		public DayOfWeek dayOfWeek() {
			return usesFieldRef() ? DayOfWeek.dayOfWeek(fieldReference) : DayOfWeek.dayOfWeek(expression);
		}

		/**
		 * Creates new {@link AggregationExpressions} that returns the year portion of a date.
		 *
		 * @return
		 */
		public Year year() {
			return usesFieldRef() ? Year.yearOf(fieldReference) : Year.yearOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpressions} that returns the month of a date as a number between 1 and 12.
		 *
		 * @return
		 */
		public Month month() {
			return usesFieldRef() ? Month.monthOf(fieldReference) : Month.monthOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpressions} that returns the week of the year for a date as a number between 0 and
		 * 53.
		 *
		 * @return
		 */
		public Week week() {
			return usesFieldRef() ? Week.weekOf(fieldReference) : Week.weekOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpressions} that returns the hour portion of a date as a number between 0 and 23.
		 *
		 * @return
		 */
		public Hour hour() {
			return usesFieldRef() ? Hour.hourOf(fieldReference) : Hour.hourOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpressions} that returns the minute portion of a date as a number between 0 and
		 * 59.
		 *
		 * @return
		 */
		public Minute minute() {
			return usesFieldRef() ? Minute.minuteOf(fieldReference) : Minute.minuteOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpressions} that returns the second portion of a date as a number between 0 and
		 * 59, but can be 60 to account for leap seconds.
		 *
		 * @return
		 */
		public Second second() {
			return usesFieldRef() ? Second.secondOf(fieldReference) : Second.secondOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpressions} that returns the millisecond portion of a date as an integer between 0
		 * and 999.
		 *
		 * @return
		 */
		public Millisecond millisecond() {
			return usesFieldRef() ? Millisecond.millisecondOf(fieldReference) : Millisecond.millisecondOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpressions} that converts a date object to a string according to a user-specified
		 * {@literal format}.
		 *
		 * @param format must not be {@literal null}.
		 * @return
		 */
		public DateToString toString(String format) {
			return (usesFieldRef() ? DateToString.dateOf(fieldReference) : DateToString.dateOf(expression)).toString(format);
		}

		/**
		 * Creates new {@link AggregationExpressions} that returns the weekday number in ISO 8601 format, ranging from 1
		 * (for Monday) to 7 (for Sunday).
		 *
		 * @return
		 */
		public IsoDayOfWeek isoDayOfWeek() {
			return usesFieldRef() ? IsoDayOfWeek.isoDayOfWeek(fieldReference) : IsoDayOfWeek.isoDayOfWeek(expression);
		}

		/**
		 * Creates new {@link AggregationExpressions} that returns the week number in ISO 8601 format, ranging from 1 to 53.
		 *
		 * @return
		 */
		public IsoWeek isoWeek() {
			return usesFieldRef() ? IsoWeek.isoWeekOf(fieldReference) : IsoWeek.isoWeekOf(expression);
		}

		/**
		 * Creates new {@link AggregationExpressions} that returns the year number in ISO 8601 format.
		 *
		 * @return
		 */
		public IsoWeekYear isoWeekYear() {
			return usesFieldRef() ? IsoWeekYear.isoWeekYearOf(fieldReference) : IsoWeekYear.isoWeekYearOf(expression);
		}

		private boolean usesFieldRef() {
			return fieldReference != null;
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $dayOfYear}.
	 *
	 * @author Christoph Strobl
	 */
	public static class DayOfYear extends AbstractAggregationExpression {

		private DayOfYear(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$dayOfYear";
		}

		/**
		 * Creates new {@link DayOfYear}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static DayOfYear dayOfYear(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new DayOfYear(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link DayOfYear}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static DayOfYear dayOfYear(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new DayOfYear(expression);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $dayOfMonth}.
	 *
	 * @author Christoph Strobl
	 */
	public static class DayOfMonth extends AbstractAggregationExpression {

		private DayOfMonth(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$dayOfMonth";
		}

		/**
		 * Creates new {@link DayOfMonth}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static DayOfMonth dayOfMonth(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new DayOfMonth(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link DayOfMonth}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static DayOfMonth dayOfMonth(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new DayOfMonth(expression);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $dayOfWeek}.
	 *
	 * @author Christoph Strobl
	 */
	public static class DayOfWeek extends AbstractAggregationExpression {

		private DayOfWeek(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$dayOfWeek";
		}

		/**
		 * Creates new {@link DayOfWeek}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static DayOfWeek dayOfWeek(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new DayOfWeek(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link DayOfWeek}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static DayOfWeek dayOfWeek(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new DayOfWeek(expression);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $year}.
	 *
	 * @author Christoph Strobl
	 */
	public static class Year extends AbstractAggregationExpression {

		private Year(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$year";
		}

		/**
		 * Creates new {@link Year}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Year yearOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Year(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Year}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Year yearOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Year(expression);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $month}.
	 *
	 * @author Christoph Strobl
	 */
	public static class Month extends AbstractAggregationExpression {

		private Month(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$month";
		}

		/**
		 * Creates new {@link Month}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Month monthOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Month(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Month}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Month monthOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Month(expression);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $week}.
	 *
	 * @author Christoph Strobl
	 */
	public static class Week extends AbstractAggregationExpression {

		private Week(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$week";
		}

		/**
		 * Creates new {@link Week}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Week weekOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Week(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Week}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Week weekOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Week(expression);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $hour}.
	 *
	 * @author Christoph Strobl
	 */
	public static class Hour extends AbstractAggregationExpression {

		private Hour(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$hour";
		}

		/**
		 * Creates new {@link Hour}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Hour hourOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Hour(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Hour}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Hour hourOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Hour(expression);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $minute}.
	 *
	 * @author Christoph Strobl
	 */
	public static class Minute extends AbstractAggregationExpression {

		private Minute(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$minute";
		}

		/**
		 * Creates new {@link Minute}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Minute minuteOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Minute(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Minute}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Minute minuteOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Minute(expression);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $second}.
	 *
	 * @author Christoph Strobl
	 */
	public static class Second extends AbstractAggregationExpression {

		private Second(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$second";
		}

		/**
		 * Creates new {@link Second}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Second secondOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Second(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Second}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Second secondOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Second(expression);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $millisecond}.
	 *
	 * @author Christoph Strobl
	 */
	public static class Millisecond extends AbstractAggregationExpression {

		private Millisecond(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$millisecond";
		}

		/**
		 * Creates new {@link Millisecond}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Millisecond millisecondOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Millisecond(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Millisecond}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Millisecond millisecondOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Millisecond(expression);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $dateToString}.
	 *
	 * @author Christoph Strobl
	 */
	public static class DateToString extends AbstractAggregationExpression {

		private DateToString(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$dateToString";
		}

		/**
		 * Creates new {@link FormatBuilder} allowing to define the date format to apply.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static FormatBuilder dateOf(final String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");

			return new FormatBuilder() {

				@Override
				public DateToString toString(String format) {

					Assert.notNull(format, "Format must not be null!");
					return new DateToString(argumentMap(Fields.field(fieldReference), format));
				}
			};
		}

		/**
		 * Creates new {@link FormatBuilder} allowing to define the date format to apply.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static FormatBuilder dateOf(final AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");

			return new FormatBuilder() {

				@Override
				public DateToString toString(String format) {

					Assert.notNull(format, "Format must not be null!");
					return new DateToString(argumentMap(expression, format));
				}
			};
		}

		private static java.util.Map<String, Object> argumentMap(Object date, String format) {

			java.util.Map<String, Object> args = new LinkedHashMap<String, Object>(2);
			args.put("format", format);
			args.put("date", date);
			return args;
		}

		public interface FormatBuilder {

			/**
			 * Creates new {@link DateToString} with all previously added arguments appending the given one.
			 *
			 * @param format must not be {@literal null}.
			 * @return
			 */
			DateToString toString(String format);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $isoDayOfWeek}.
	 *
	 * @author Christoph Strobl
	 */
	public static class IsoDayOfWeek extends AbstractAggregationExpression {

		private IsoDayOfWeek(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$isoDayOfWeek";
		}

		/**
		 * Creates new {@link IsoDayOfWeek}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static IsoDayOfWeek isoDayOfWeek(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new IsoDayOfWeek(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link IsoDayOfWeek}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static IsoDayOfWeek isoDayOfWeek(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new IsoDayOfWeek(expression);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $isoWeek}.
	 *
	 * @author Christoph Strobl
	 */
	public static class IsoWeek extends AbstractAggregationExpression {

		private IsoWeek(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$isoWeek";
		}

		/**
		 * Creates new {@link IsoWeek}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static IsoWeek isoWeekOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new IsoWeek(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link IsoWeek}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static IsoWeek isoWeekOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new IsoWeek(expression);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $isoWeekYear}.
	 *
	 * @author Christoph Strobl
	 */
	public static class IsoWeekYear extends AbstractAggregationExpression {

		private IsoWeekYear(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$isoWeekYear";
		}

		/**
		 * Creates new {@link IsoWeekYear}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static IsoWeekYear isoWeekYearOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new IsoWeekYear(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Millisecond}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static IsoWeekYear isoWeekYearOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new IsoWeekYear(expression);
		}
	}
}
