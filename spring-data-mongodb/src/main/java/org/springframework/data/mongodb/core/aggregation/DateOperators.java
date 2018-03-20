/*
 * Copyright 2016-2018. the original author or authors.
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

import static org.springframework.data.mongodb.core.aggregation.ConditionalOperators.*;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.data.mongodb.core.aggregation.ArithmeticOperators.ArithmeticOperatorFactory;
import org.springframework.data.mongodb.core.aggregation.ConditionalOperators.*;
import org.springframework.util.Assert;

import com.mongodb.DBObject;

/**
 * Gateway to {@literal Date} aggregation operations.
 * <p>
 * Prior to Mongo 3.6, all Date operations were in the UTC timezone<br>
 * New in Mongo 3.6 is support for timezone conversion on all aggregation operations. This is a <em>breaking</em> change
 * and using any of the aggregation methods with a 'timezone' attribute on a Mongo server prior to 3.6 will cause
 * errors.
 *
 * @author Christoph Strobl
 * @author Matt Morrissette
 * @since 1.10
 */
public class DateOperators {

	/**
	 * Take the date referenced by given {@literal fieldReference} in the UTC timezone.
	 *
	 * @param fieldReference must not be {@literal null}.
	 * @return
	 */
	public static DateOperatorFactory dateOf(String fieldReference) {

		Assert.hasText(fieldReference, "FieldReference must not be null!");
		return new DateOperatorFactory(fieldReference, null);
	}

	/**
	 * Take the date referenced by given {@literal fieldReference} in the given timezone.
	 * <p>
	 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
	 *
	 * @param fieldReference must not be {@literal null}.
	 * @param timezone nullable. The timezone ID or offset. If null, UTC is assumed.
	 * @since 2.1
	 * @return
	 */
	public static DateOperatorFactory dateOfWithTimezone(String fieldReference, String timezone) {

		Assert.hasText(fieldReference, "FieldReference must not be null!");
		return new DateOperatorFactory(fieldReference, timezone);
	}

	/**
	 * Take the date referenced by given {@literal fieldReference} in the given timezone.
	 * <p>
	 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
	 *
	 * @param fieldReference must not be {@literal null}.
	 * @param timezoneExpression Must not be null. The expression to bind the timezone value from
	 * @since 2.1
	 * @return
	 */
	public static DateOperatorFactory dateOfWithTimezoneOf(String fieldReference,
			AggregationExpression timezoneExpression) {

		Assert.hasText(fieldReference, "FieldReference must not be null!");
		Assert.notNull(timezoneExpression, "timezoneExpression must not be null!");
		return new DateOperatorFactory(fieldReference, timezoneExpression);
	}

	/**
	 * Take the date referenced by given {@literal fieldReference} in the given timezone.
	 * <p>
	 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
	 *
	 * @param fieldReference must not be {@literal null}. The field to bind the date value from
	 * @param timezoneField Must not be null. The name of the field to bind the timezone value from
	 * @since 2.1
	 * @return
	 */
	public static DateOperatorFactory dateOfWithTimezoneOf(String fieldReference, String timezoneField) {

		Assert.hasText(fieldReference, "FieldReference must not be null!");
		return new DateOperatorFactory(fieldReference, Fields.field(timezoneField));
	}

	/**
	 * Take the date resulting from the given {@link AggregationExpression} in the UTC timezone.
	 *
	 * @param expression must not be {@literal null}.
	 * @return
	 */
	public static DateOperatorFactory dateOf(AggregationExpression expression) {
		return DateOperators.dateOfWithTimezone(expression, null);
	}

	/**
	 * Take the date resulting from the given {@link AggregationExpression} in the given timezone.
	 * <p>
	 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
	 *
	 * @param expression must not be {@literal null}.
	 * @param timezone nullable. The timezone ID or offset. If null, UTC is assumed.
	 * @since 2.1
	 * @return
	 */
	public static DateOperatorFactory dateOfWithTimezone(AggregationExpression expression, String timezone) {

		Assert.notNull(expression, "Expression must not be null!");
		return new DateOperatorFactory(expression, timezone);
	}

	/**
	 * Take the date referenced by given {@link AggregationExpression} in the given timezone.
	 * <p>
	 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
	 *
	 * @param expression must not be {@literal null}.
	 * @param timezoneExpression Must not be null. The expression to bind the timezone value from
	 * @since 2.1
	 * @return
	 */
	public static DateOperatorFactory dateOfWithTimezoneOf(AggregationExpression expression,
			AggregationExpression timezoneExpression) {

		Assert.notNull(expression, "expression must not be null!");
		Assert.notNull(timezoneExpression, "timezoneExpression must not be null!");
		return new DateOperatorFactory(expression, timezoneExpression);
	}

	/**
	 * Take the date referenced by given {@link AggregationExpression} in the given timezone.
	 * <p>
	 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
	 *
	 * @param expression must not be {@literal null}.
	 * @param timezoneField Must not be null. The name of the field to bind the timezone value from
	 * @since 2.1
	 * @return
	 */
	public static DateOperatorFactory dateOfWithTimezoneOf(AggregationExpression expression, String timezoneField) {

		Assert.notNull(expression, "expression must not be null!");
		return new DateOperatorFactory(expression, Fields.field(timezoneField));
	}

	/**
	 * Take the current date as supplied by the {@link DateFactory} in the UTC timezone
	 *
	 * @param factory not nullable. The DateFactory to get the current date from
	 * @since 2.1
	 * @return
	 */
	public static DateOperatorFactory dateOf(DateFactory factory) {
		return new DateOperatorFactory(factory, null);
	}

	/**
	 * Take the current date resulting from the given {@link DateFactory} in the given timezone.
	 * <p>
	 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors. If using Mongo prior to 3.6,
	 * specify timezone as null.
	 *
	 * @param factory not nullable. The DateFactory to get the current date from
	 * @param timezone nullable. The timezone ID or offset. If null, UTC is assumed. Must specify as null if using Mongo
	 *          prior to 3.6
	 * @since 2.1
	 * @return
	 */
	public static DateOperatorFactory dateOfWithTimezone(DateFactory factory, String timezone) {
		return new DateOperatorFactory(factory, timezone);
	}

	/**
	 * Take the date referenced by given {@link DateFactory} in the given timezone.
	 * <p>
	 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
	 *
	 * @param factory not nullable. The DateFactory to get the current date from
	 * @param timezoneExpression Must not be null. The expression to bind the timezone value from
	 * @since 2.1
	 * @return
	 */
	public static DateOperatorFactory dateOfWithTimezoneOf(DateFactory factory,
			AggregationExpression timezoneExpression) {

		Assert.notNull(factory, "Factory must not be null!");
		Assert.notNull(timezoneExpression, "timezoneExpression must not be null!");
		return new DateOperatorFactory(factory, timezoneExpression);
	}

	/**
	 * Take the date referenced by given {@link DateFactory}in the given timezone.
	 * <p>
	 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
	 *
	 * @param factory {@link DateFactory#LOCAL_DATE_FACTORY}. Defaults to {@link DateOperators#getCurrentDateFactory} if
	 *          null.
	 * @param timezoneField Must not be null. The name of the field to bind the timezone value from
	 * @since 2.1
	 * @return
	 */
	public static DateOperatorFactory dateOfWithTimezoneOf(DateFactory factory, String timezoneField) {

		Assert.notNull(factory, "Factory must not be null!");
		Assert.notNull(timezoneField, "timezoneField must not be null!");
		return new DateOperatorFactory(factory, Fields.field(timezoneField));
	}

	/**
	 * Take the current date using the default {@link DateOperators#getCurrentDateFactory()} in the UTC timezone.
	 *
	 * @since 2.1
	 * @return
	 */
	public static DateOperatorFactory currentDate() {
		return dateOf(CURRENT_DATE_FACTORY);
	}

	/**
	 * Take the current date using the default {@link DateOperators#getCurrentDateFactory()} in the given timezone.
	 * <p>
	 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
	 *
	 * @param timezone nullable. The timezone ID or offset. If null, UTC is assumed.
	 * @since 2.1
	 * @return
	 */
	public static DateOperatorFactory currentDateWithTimezone(String timezone) {
		return DateOperators.dateOfWithTimezone(CURRENT_DATE_FACTORY, timezone);
	}

	/**
	 * Take the current date using the default {@link DateOperators#getCurrentDateFactory()} in the given timezone.
	 * <p>
	 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
	 *
	 * @param timezoneExpression Must not be null. The name of the field to bind the timezone value from
	 * @since 2.1
	 * @return
	 */
	public static DateOperatorFactory currentDateWithTimezoneOf(AggregationExpression timezoneExpression) {
		return DateOperators.dateOfWithTimezoneOf(CURRENT_DATE_FACTORY, timezoneExpression);
	}

	/**
	 * Take the current date using the default {@link DateOperators#getCurrentDateFactory()} in the given timezone.
	 * <p>
	 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
	 *
	 * @param timezoneField Must not be null. The name of the field to bind the timezone value from
	 * @since 2.1
	 * @return
	 */
	public static DateOperatorFactory currentDateWithTimezoneOf(String timezoneField) {
		return DateOperators.dateOfWithTimezoneOf(CURRENT_DATE_FACTORY, timezoneField);
	}

	/**
	 * @see DateFromParts#fromParts
	 * @since 2.1
	 * @author Matt Morrissette
	 * @return
	 */
	public static DateFromParts.CalendarDatePartsBuilder dateFromParts() {
		return DateFromParts.fromParts();
	}

	/**
	 * @see DateFromParts#fromIsoWeekParts
	 * @since 2.1
	 * @author Matt Morrissette
	 * @return
	 */
	public static DateFromParts.IsoWeekDatePartsBuilder dateFromIsoWeekParts() {
		return DateFromParts.fromIsoWeekParts();
	}

	/**
	 * @author Christoph Strobl
	 * @author Matt Morrissette
	 */
	public static class DateOperatorFactory {

		private final String fieldReference;
		private final AggregationExpression expression;
		private final DateFactory dateFactory;
		private final Object timezone;

		/**
		 * Creates new {@link ArithmeticOperatorFactory} for given {@literal fieldReference} in the UTC timezone.
		 *
		 * @param fieldReference must not be {@literal null}.
		 */
		public DateOperatorFactory(String fieldReference) {
			this(fieldReference, null);
		}

		private DateOperatorFactory(String fieldReference, Object timezone) {
			Assert.hasText(fieldReference, "FieldReference must not be null!");
			this.fieldReference = fieldReference;
			this.expression = null;
			this.dateFactory = null;
			this.timezone = timezone;
		}

		/**
		 * Creates new {@link ArithmeticOperatorFactory} for given {@link AggregationExpression} in the UTC timezone.
		 *
		 * @param expression must not be {@literal null}.
		 */
		public DateOperatorFactory(AggregationExpression expression) {
			this(expression, null);
		}

		private DateOperatorFactory(AggregationExpression expression, Object timezone) {

			Assert.notNull(expression, "Expression must not be null!");
			this.fieldReference = null;
			this.expression = expression;
			this.timezone = timezone;
			this.dateFactory = null;
		}

		private DateOperatorFactory(DateFactory dateFactory, Object timezone) {

			this.fieldReference = null;
			this.expression = null;
			this.timezone = timezone;
			this.dateFactory = dateFactory != null ? dateFactory : CURRENT_DATE_FACTORY;
		}

		private DateOperatorFactory(String fieldRefererence, AggregationExpression expression, DateFactory dateFactory,
				Object timezone) {

			this.fieldReference = fieldRefererence;
			this.expression = expression;
			this.dateFactory = dateFactory;
			this.timezone = timezone;
		}

		/**
		 * @param timezone nullable. The timezone ID or offset as a String.
		 * @return a new DateOperator factory with the same date reference/expression/factory but with the given timezone
		 */
		public DateOperatorFactory withTimezone(String timezone) {
			return new DateOperatorFactory(fieldReference, expression, dateFactory, timezone);
		}

		/**
		 * @param timezoneField not nullable. The field reference to bind the timezone from.
		 * @return a new DateOperator factory with the same date reference/expression/factory but with the given timezone
		 */
		public DateOperatorFactory withTimezoneOf(String timezoneField) {

			Assert.hasText(timezoneField, "timezoneField cannot be null or empty");
			return new DateOperatorFactory(fieldReference, expression, dateFactory, Fields.field(timezoneField));
		}

		/**
		 * @param timezoneExpression not nullable. The expression to bind the timezone from
		 * @return a new DateOperator factory with the same date reference/expression/factory but with the given timezone
		 */
		public DateOperatorFactory withTimezoneOf(AggregationExpression timezoneExpression) {

			Assert.notNull(timezoneExpression, "timezoneExpression cannot be null or empty");
			return new DateOperatorFactory(fieldReference, expression, dateFactory, timezoneExpression);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the day of the year for a date as a number between 1 and
		 * 366 in the factory timezone (default UTC).
		 *
		 * @return
		 */
		public DayOfYear dayOfYear() {
			return dayOfYear(timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the day of the year for a date as a number between 1 and
		 * 366 in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param timezone nullable. Overrides factory timezone. The timezone ID or offset as a String. Also accepts a
		 *          {@link AggregationExpression} or {@link Field}. If null UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public DayOfYear dayOfYear(Object timezone) {
			return usesFieldRef() ? DayOfYear.dayOfYear(fieldReference, timezone)
					: usesExpression() ? DayOfYear.dayOfYear(expression, timezone) : DayOfYear.dayOfYear(dateFactory, timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the day of the month for a date as a number between 1 and
		 * 31 in the factory timezone (default UTC).
		 *
		 * @return
		 */
		public DayOfMonth dayOfMonth() {
			return dayOfMonth(timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the day of the month for a date as a number between 1 and
		 * 31 in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param timezone nullable. Overrides factory timezone. The timezone ID or offset as a String. Also accepts a
		 *          {@link AggregationExpression} or {@link Field}. If null UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public DayOfMonth dayOfMonth(Object timezone) {
			return usesFieldRef() ? DayOfMonth.dayOfMonth(fieldReference, timezone)
					: usesExpression() ? DayOfMonth.dayOfMonth(expression, timezone)
							: DayOfMonth.dayOfMonth(dateFactory, timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the day of the week for a date as a number between 1
		 * (Sunday) and 7 (Saturday) in the factory timezone (default UTC).
		 *
		 * @return
		 */
		public DayOfWeek dayOfWeek() {
			return dayOfWeek(timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the day of the week for a date as a number between 1
		 * (Sunday) and 7 (Saturday) in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param timezone nullable. Overrides factory timezone. The timezone ID or offset as a String. Also accepts a
		 *          {@link AggregationExpression} or {@link Field}. If null UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public DayOfWeek dayOfWeek(Object timezone) {
			return usesFieldRef() ? DayOfWeek.dayOfWeek(fieldReference, timezone)
					: usesExpression() ? DayOfWeek.dayOfWeek(expression, timezone) : DayOfWeek.dayOfWeek(dateFactory, timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the year portion of a date in the factory timezone
		 * (default UTC).
		 *
		 * @return
		 */
		public Year year() {
			return year(timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the year portion of a date in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param timezone nullable. Overrides factory timezone. The timezone ID or offset as a String. Also accepts a
		 *          {@link AggregationExpression} or {@link Field}. If null UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public Year year(Object timezone) {
			return usesFieldRef() ? Year.yearOf(fieldReference, timezone)
					: usesExpression() ? Year.yearOf(expression, timezone) : Year.yearOf(dateFactory, timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the quarter of a date as a number between 1 and 4 in the
		 * factory timezone (default UTC).
		 *
		 * @return
		 */
		public Quarter quarter() {
			return quarter(timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the business quarter of a date as a number between 1 and 4
		 * in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param timezone nullable. Overrides factory timezone. The timezone ID or offset as a String. Also accepts a
		 *          {@link AggregationExpression} or {@link Field}. If null UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public Quarter quarter(Object timezone) {
			return usesFieldRef() ? Quarter.quarterOf(fieldReference, timezone)
					: usesExpression() ? Quarter.quarterOf(expression, timezone) : Quarter.quarterOf(dateFactory, timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the month of a date as a number between 1 and 12 in the
		 * factory timezone (default UTC).
		 *
		 * @return
		 */
		public Month month() {
			return month(timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the month of a date as a number between 1 and 12 in the
		 * given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param timezone nullable. Overrides factory timezone. The timezone ID or offset as a String. Also accepts a
		 *          {@link AggregationExpression} or {@link Field}. If null UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public Month month(Object timezone) {
			return usesFieldRef() ? Month.monthOf(fieldReference, timezone)
					: usesExpression() ? Month.monthOf(expression, timezone) : Month.monthOf(dateFactory, timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the week of the year for a date as a number between 0 and
		 * 53 in the factory timezone (default UTC).
		 *
		 * @return
		 */
		public Week week() {
			return week(timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the week of the year for a date as a number between 0 and
		 * 53 in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param timezone nullable. The timezone ID or offset. If null, UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public Week week(Object timezone) {
			return usesFieldRef() ? Week.weekOf(fieldReference, timezone)
					: usesExpression() ? Week.weekOf(expression, timezone) : Week.weekOf(dateFactory, timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the hour portion of a date as a number between 0 and 23 in
		 * the factory timezone (default UTC).
		 *
		 * @return
		 */
		public Hour hour() {
			return hour(timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the hour portion of a date as a number between 0 and 23 in
		 * the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param timezone nullable. Overrides factory timezone. The timezone ID or offset as a String. Also accepts a
		 *          {@link AggregationExpression} or {@link Field}. If null UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public Hour hour(Object timezone) {
			return usesFieldRef() ? Hour.hourOf(fieldReference, timezone)
					: usesExpression() ? Hour.hourOf(expression, timezone) : Hour.hourOf(dateFactory, timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the minute portion of a date as a number between 0 and 59
		 * in the factory timezone (default UTC).
		 *
		 * @return
		 */
		public Minute minute() {
			return minute(timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the minute portion of a date as a number between 0 and 59
		 * in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param timezone nullable. Overrides factory timezone. The timezone ID or offset as a String. Also accepts a
		 *          {@link AggregationExpression} or {@link Field}. If null UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public Minute minute(Object timezone) {
			return usesFieldRef() ? Minute.minuteOf(fieldReference, timezone)
					: usesExpression() ? Minute.minuteOf(expression, timezone) : Minute.minuteOf(dateFactory, timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the second portion of a date as a number between 0 and 59,
		 * but can be 60 to account for leap seconds in the factory timezone (default UTC).
		 *
		 * @return
		 */
		public Second second() {
			return second(timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the second portion of a date as a number between 0 and 59,
		 * but can be 60 to account for leap seconds in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param timezone nullable. Overrides factory timezone. The timezone ID or offset as a String. Also accepts a
		 *          {@link AggregationExpression} or {@link Field}. If null UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public Second second(Object timezone) {
			return usesFieldRef() ? Second.secondOf(fieldReference, timezone)
					: usesExpression() ? Second.secondOf(expression, timezone) : Second.secondOf(dateFactory, timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the millisecond portion of a date as an integer between 0
		 * and 999 in the factory timezone (default UTC).
		 *
		 * @return
		 */
		public Millisecond millisecond() {
			return millisecond(timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the millisecond portion of a date as an integer between 0
		 * and 999 in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param timezone nullable. Overrides factory timezone. The timezone ID or offset as a String. Also accepts a
		 *          {@link AggregationExpression} or {@link Field}. If null UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public Millisecond millisecond(Object timezone) {
			return usesFieldRef() ? Millisecond.millisecondOf(fieldReference, timezone)
					: usesExpression() ? Millisecond.millisecondOf(expression, timezone)
							: Millisecond.millisecondOf(dateFactory, timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the weekday number in ISO 8601 format, ranging from 1 (for
		 * Monday) to 7 (for Sunday) in the factory timezone (default UTC).
		 *
		 * @return
		 */
		public IsoDayOfWeek isoDayOfWeek() {
			return isoDayOfWeek(timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the weekday number in ISO 8601-2018 format, ranging from 1
		 * (for Monday) to 7 (for Sunday) in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param timezone nullable. Overrides factory timezone. The timezone ID or offset as a String. Also accepts a
		 *          {@link AggregationExpression} or {@link Field}. If null UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public IsoDayOfWeek isoDayOfWeek(Object timezone) {
			return usesFieldRef() ? IsoDayOfWeek.isoDayOfWeek(fieldReference, timezone)
					: usesExpression() ? IsoDayOfWeek.isoDayOfWeek(expression, timezone)
							: IsoDayOfWeek.isoDayOfWeek(dateFactory, timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the week number in ISO 8601 format, ranging from 1 to 53
		 * in the factory timezone (default UTC).
		 *
		 * @return
		 */
		public IsoWeek isoWeek() {
			return isoWeek(timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the week number in ISO 8601-2018 format, ranging from 1 to
		 * 53 in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param timezone nullable. Overrides factory timezone. The timezone ID or offset as a String. Also accepts a
		 *          {@link AggregationExpression} or {@link Field}. If null UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public IsoWeek isoWeek(Object timezone) {
			return usesFieldRef() ? IsoWeek.isoWeekOf(fieldReference, timezone)
					: usesExpression() ? IsoWeek.isoWeekOf(expression, timezone) : IsoWeek.isoWeekOf(dateFactory, timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the year number in ISO 8601 format in the factory timezone
		 * (default UTC).
		 *
		 * @return
		 */
		public IsoWeekYear isoWeekYear() {
			return isoWeekYear(timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the year number in ISO 8601-2018 format in the given
		 * timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param timezone nullable. Overrides factory timezone. The timezone ID or offset as a String. Also accepts a
		 *          {@link AggregationExpression} or {@link Field}. If null UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public IsoWeekYear isoWeekYear(Object timezone) {
			return usesFieldRef() ? IsoWeekYear.isoWeekYearOf(fieldReference, timezone)
					: usesExpression() ? IsoWeekYear.isoWeekYearOf(expression, timezone)
							: IsoWeekYear.isoWeekYearOf(dateFactory, timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that converts a date object to a string according to a user-specified
		 * {@literal format} in the factory timezone (default UTC).
		 *
		 * @param format must not be {@literal null}.
		 * @since 2.1
		 * @return
		 */
		public DateToString toString(String format) {
			return toString(format, timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that converts a date object to a string according to a user-specified
		 * {@literal format} in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param format must not be {@literal null}.
		 * @param timezone nullable. Overrides factory timezone. The timezone ID or offset as a String. Also accepts a
		 *          {@link AggregationExpression} or {@link Field}. If null UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public DateToString toString(String format, Object timezone) {
			return (usesFieldRef() ? DateToString.dateOf(fieldReference, timezone)
					: usesExpression() ? DateToString.dateOf(expression, timezone) : DateToString.dateOf(dateFactory, timezone))
							.toString(format);
		}

		/**
		 * Creates new {@link AggregationExpression} that converts a string to a date object in the factory timezone
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @since 2.1
		 * @return
		 */
		public DateFromString fromString() {
			return fromString(timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that converts a string to a date object in the given timezone
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param timezone nullable. Overrides factory timezone. The timezone ID or offset as a String. Also accepts a
		 *          {@link AggregationExpression} or {@link Field}. If null UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public DateFromString fromString(Object timezone) {
			return usesFieldRef() ? DateFromString.dateFromString(fieldReference, timezone)
					: usesExpression() ? DateFromString.dateFromString(expression, timezone)
							: DateFromString.dateFromString(dateFactory, timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that converts a string to a date object in the factory timezone using
		 * calendar parts (year/month/day)
		 * <p>
		 * WARNING: Mongo 3.6+ only
		 *
		 * @since 2.1
		 * @return
		 */
		public DateToParts toParts() {
			return toParts(timezone, null);
		}

		/**
		 * Creates new {@link AggregationExpression} that converts a string to a date object in the factory timezone using
		 * isoWeek parts (isoWeekYear/isoWeek/isoDayOfWeek)
		 * <p>
		 * WARNING: Mongo 3.6+ only
		 *
		 * @since 2.1
		 * @return
		 */
		public DateToParts toIsoWeekParts() {
			return toParts(timezone, true);
		}

		/**
		 * Creates new {@link AggregationExpression} that converts a string to a date object in the factory timezone
		 * <p>
		 * WARNING: Mongo 3.6+ only
		 *
		 * @param iso8601 If set to true, modifies the output document to use ISO week date fields. Defaults to false.
		 * @since 2.1
		 * @return
		 */
		public DateToParts toParts(Boolean iso8601) {
			return toParts(timezone, iso8601);
		}

		/**
		 * Creates new {@link AggregationExpression} that converts a string to a date object in the given timezone using
		 * calendar parts (year/month/day)
		 * <p>
		 * WARNING: Mongo 3.6+ only
		 *
		 * @param timezone nullable. Overrides factory timezone. The timezone ID or offset as a String. Also accepts a
		 *          {@link AggregationExpression} or {@link Field}. If null UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public DateToParts toParts(Object timezone) {
			return toParts(timezone, null);
		}

		/**
		 * Creates new {@link AggregationExpression} that converts a string to a date object in the given timezone using
		 * isoWeek parts (isoWeekYear/isoWeek/isoDayOfWeek)
		 * <p>
		 * WARNING: Mongo 3.6+ only
		 *
		 * @param timezone nullable. Overrides factory timezone. The timezone ID or offset as a String. Also accepts a
		 *          {@link AggregationExpression} or {@link Field}. If null UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public DateToParts toIsoWeekParts(Object timezone) {
			return toParts(timezone, true);
		}

		/**
		 * Creates new {@link AggregationExpression} that converts a string to a date object in the given timezone
		 * <p>
		 * WARNING: Mongo 3.6+ only
		 *
		 * @param timezone nullable. Overrides factory timezone. The timezone ID or offset as a String. Also accepts a
		 *          {@link AggregationExpression} or {@link Field}. If null UTC is assumed.
		 * @param iso8601 If set to true, modifies the output document to use ISO week date fields
		 *          (isoWeekYear/isoWeek/isoDayOfWeek). Defaults to false.
		 * @since 2.1
		 * @return
		 */
		public DateToParts toParts(Object timezone, Boolean iso8601) {
			return usesFieldRef() ? DateToParts.dateToParts(fieldReference, timezone, iso8601)
					: usesExpression() ? DateToParts.dateToParts(expression, timezone, iso8601)
							: DateToParts.dateToParts(dateFactory, timezone, iso8601);
		}

		private boolean usesFieldRef() {
			return fieldReference != null;
		}

		private boolean usesExpression() {
			return expression != null;
		}
	}

	// contemplates support for the future functionality described in:
	// https://jira.mongodb.org/browse/SERVER-23656
	private static DateFactory CURRENT_DATE_FACTORY = new DateFactory() {
		@Override
		public Object currentDate() {
			return new Date();
		}
	};

	/**
	 * Sets the {@link DateFactory} used by {@link DateOperators#currentDate()} and
	 * {@link DateOperators#currentDate(java.lang.String)} for the entire application (statically).
	 *
	 * @param defaultFactory
	 */
	public static void setCurrentDateFactory(final DateFactory defaultFactory) {

		Assert.notNull(defaultFactory, "Default DateFactory cannot be null");
		CURRENT_DATE_FACTORY = defaultFactory;
	}

	/**
	 * @return the {@link DateFactory} used by {@link DateOperators#currentDate()} and
	 *         {@link DateOperators#currentDate(java.lang.String)}
	 */
	public static DateFactory getCurrentDateFactory() {
		return CURRENT_DATE_FACTORY;
	}

	/**
	 * New in Mongo 3.6 is support for timezone specification with all date types.
	 * <p>
	 * WARNING: Using timezone requires Mongo 3.6+ and will error on prior versions of Mongo
	 *
	 * @since 2.1
	 */
	private abstract static class DateAggregationExpression extends AbstractAggregationExpression {

		protected DateAggregationExpression(Object date, Object timezone) {
			super(arguments(date, timezone));
		}

		private static Object arguments(Object date, Object timezone) {

			if (timezone != null) {
				Assert.isTrue(DateAggregationExpression.isValidTimezoneObject(timezone),
						"Timezone was not a valid timezone: " + timezone + ". Must be String, AggregationExpression or Field");

				java.util.Map<String, Object> args = new LinkedHashMap<String, Object>(4);
				args.put("date", date);
				args.put("timezone", timezone);
				return args;
			} else {
				return date;
			}
		}

		public static boolean isValidTimezoneObject(Object timezone) {
			return timezone == null
					|| (timezone instanceof String || timezone instanceof Field || timezone instanceof AggregationExpression);
		}

	}

	/**
	 * {@link AggregationExpression} for {@code $dayOfYear}.
	 *
	 * @author Christoph Strobl
	 * @see https://docs.mongodb.com/manual/reference/operator/aggregation/dayOfYear/
	 */
	public static class DayOfYear extends DateAggregationExpression {

		private DayOfYear(Object value, Object timezone) {
			super(value, timezone);
		}

		@Override
		protected String getMongoMethod() {
			return "$dayOfYear";
		}

		/**
		 * Creates new {@link DayOfYear} in UTC.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static DayOfYear dayOfYear(String fieldReference) {
			return dayOfYear(fieldReference, null);
		}

		/**
		 * Creates new {@link DayOfYear} for the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public static DayOfYear dayOfYear(String fieldReference, Object timezone) {

			Assert.hasText(fieldReference, "FieldReference must not be null!");
			return new DayOfYear(Fields.field(fieldReference), timezone);
		}

		/**
		 * Creates new {@link DayOfYear} in UTC.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static DayOfYear dayOfYear(AggregationExpression expression) {
			return dayOfYear(expression, null);
		}

		/**
		 * Creates new {@link DayOfYear} for the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param expression must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public static DayOfYear dayOfYear(AggregationExpression expression, Object timezone) {

			Assert.notNull(expression, "Expression must not be null!");
			return new DayOfYear(expression, timezone);
		}

		/**
		 * Creates new {@link DayOfYear} for current date in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param dateFactory must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public static DayOfYear dayOfYear(DateFactory dateFactory, Object timezone) {

			Assert.notNull(dateFactory, "dateFactory must not be null!");
			return new DayOfYear(dateFactory, timezone);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $dayOfMonth}.
	 *
	 * @author Christoph Strobl
	 * @see https://docs.mongodb.com/manual/reference/operator/aggregation/dayOfMonth/
	 */
	public static class DayOfMonth extends DateAggregationExpression {

		private DayOfMonth(Object value, Object timezone) {
			super(value, timezone);
		}

		@Override
		protected String getMongoMethod() {
			return "$dayOfMonth";
		}

		/**
		 * Creates new {@link DayOfMonth} in UTC.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static DayOfMonth dayOfMonth(String fieldReference) {
			return dayOfMonth(fieldReference, null);
		}

		/**
		 * Creates new {@link DayOfMonth} in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public static DayOfMonth dayOfMonth(String fieldReference, Object timezone) {

			Assert.hasText(fieldReference, "FieldReference must not be null!");
			return new DayOfMonth(Fields.field(fieldReference), timezone);
		}

		/**
		 * Creates new {@link DayOfMonth} in UTC.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static DayOfMonth dayOfMonth(AggregationExpression expression) {
			return dayOfMonth(expression, null);
		}

		/**
		 * Creates new {@link DayOfMonth} in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param expression must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public static DayOfMonth dayOfMonth(AggregationExpression expression, Object timezone) {

			Assert.notNull(expression, "Expression must not be null!");
			return new DayOfMonth(expression, timezone);
		}

		/**
		 * Creates new {@link DayOfMonth} for current date in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors. If Mongo is less than 3.6,
		 * timezone must be null.
		 *
		 * @param dateFactory must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed. (supports Mongo prior to 3.6 if null)
		 * @since 2.1
		 * @return
		 */
		public static DayOfMonth dayOfMonth(DateFactory dateFactory, Object timezone) {

			Assert.notNull(dateFactory, "dateFactory must not be null!");
			return new DayOfMonth(dateFactory, timezone);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $dayOfWeek}.
	 *
	 * @author Christoph Strobl
	 * @see https://docs.mongodb.com/manual/reference/operator/aggregation/dayOfWeek/
	 */
	public static class DayOfWeek extends DateAggregationExpression {

		private DayOfWeek(Object value, Object timezone) {
			super(value, timezone);
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
			return dayOfWeek(fieldReference, null);
		}

		/**
		 * Creates new {@link DayOfWeek} in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public static DayOfWeek dayOfWeek(String fieldReference, Object timezone) {

			Assert.hasText(fieldReference, "FieldReference must not be null!");
			return new DayOfWeek(Fields.field(fieldReference), timezone);
		}

		/**
		 * Creates new {@link DayOfWeek}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static DayOfWeek dayOfWeek(AggregationExpression expression) {
			return dayOfWeek(expression, null);
		}

		/**
		 * Creates new {@link DayOfWeek} in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param expression must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public static DayOfWeek dayOfWeek(AggregationExpression expression, Object timezone) {

			Assert.notNull(expression, "Expression must not be null!");
			return new DayOfWeek(expression, timezone);
		}

		/**
		 * Creates new {@link DayOfWeek} for current date in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors. If Mongo is less than 3.6,
		 * timezone must be null.
		 *
		 * @param dateFactory must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed. (supports Mongo prior to 3.6 if null)
		 * @since 2.1
		 * @return
		 */
		public static DayOfWeek dayOfWeek(DateFactory dateFactory, Object timezone) {

			Assert.notNull(dateFactory, "dateFactory must not be null!");
			return new DayOfWeek(dateFactory, timezone);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $year}.
	 *
	 * @author Christoph Strobl
	 * @see https://docs.mongodb.com/manual/reference/operator/aggregation/year/
	 */
	public static class Year extends DateAggregationExpression {

		private Year(Object value, Object timezone) {
			super(value, timezone);
		}

		@Override
		protected String getMongoMethod() {
			return "$year";
		}

		/**
		 * Creates new {@link Year} in the UTC timezone.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Year yearOf(String fieldReference) {
			return yearOf(fieldReference, null);
		}

		/**
		 * Creates new {@link Year} in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public static Year yearOf(String fieldReference, Object timezone) {

			Assert.hasText(fieldReference, "FieldReference must not be null!");
			return new Year(Fields.field(fieldReference), timezone);
		}

		/**
		 * Creates new {@link Year} in the UTC timezone.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Year yearOf(AggregationExpression expression) {
			return yearOf(expression, null);
		}

		/**
		 * Creates new {@link Year} in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param expression must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public static Year yearOf(AggregationExpression expression, Object timezone) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Year(expression, timezone);
		}

		/**
		 * Creates new {@link Year} for current date in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors. If Mongo is less than 3.6,
		 * timezone must be null.
		 *
		 * @param dateFactory must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed. (supports Mongo prior to 3.6 if null)
		 * @since 2.1
		 * @return
		 */
		public static Year yearOf(DateFactory dateFactory, Object timezone) {

			Assert.notNull(dateFactory, "dateFactory must not be null!");
			return new Year(dateFactory, timezone);
		}
	}

	/**
	 * Pseudo {@link AggregationExpression} to represent the current business quarter using conditionals. Can be used in a
	 * $group aggregation state to group results by business quarter
	 *
	 * @author Matt Morrissette
	 */
	public static class Quarter implements AggregationExpression {

		private final Cond cond;

		private Quarter(Object value, Object timezone) {

			final Month month = new Month(value, timezone);
			cond = when(quarterConditional(month, 3)).then(1).otherwiseValueOf(when(quarterConditional(month, 6)).then(2)
					.otherwiseValueOf(when(quarterConditional(month, 9)).then(3).otherwise(4)));
		}

		private static AggregationExpression quarterConditional(final Month month, int mininumMonth) {
			return ComparisonOperators.valueOf(month).lessThanEqualToValue(mininumMonth);
		}

		/**
		 * Creates new {@link Quarter} in the UTC timezone.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Quarter quarterOf(String fieldReference) {
			return quarterOf(fieldReference, null);
		}

		/**
		 * Creates new {@link Quarter} in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public static Quarter quarterOf(String fieldReference, Object timezone) {

			Assert.hasText(fieldReference, "FieldReference must not be null!");
			return new Quarter(Fields.field(fieldReference), timezone);
		}

		/**
		 * Creates new {@link Quarter} in the UTC timezone.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Quarter quarterOf(AggregationExpression expression) {
			return quarterOf(expression, null);
		}

		/**
		 * Creates new {@link Quarter} in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param expression must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public static Quarter quarterOf(AggregationExpression expression, Object timezone) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Quarter(expression, timezone);
		}

		/**
		 * Creates new {@link Quarter} for current date in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors. If Mongo is less than 3.6,
		 * timezone must be null.
		 *
		 * @param dateFactory must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed. (supports Mongo prior to 3.6 if null)
		 * @since 2.1
		 * @return
		 */
		public static Quarter quarterOf(DateFactory dateFactory, Object timezone) {

			Assert.notNull(dateFactory, "dateFactory must not be null!");
			return new Quarter(dateFactory, timezone);
		}

		@Override
		public DBObject toDbObject(AggregationOperationContext context) {
			return cond.toDbObject(context);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $month}.
	 *
	 * @author Christoph Strobl
	 * @see https://docs.mongodb.com/manual/reference/operator/aggregation/month/
	 */
	public static class Month extends DateAggregationExpression {

		private Month(Object value, Object timezone) {
			super(value, timezone);
		}

		@Override
		protected String getMongoMethod() {
			return "$month";
		}

		/**
		 * Creates new {@link Month} in the UTC timezone.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Month monthOf(String fieldReference) {
			return monthOf(fieldReference, null);
		}

		/**
		 * Creates new {@link Month} in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public static Month monthOf(String fieldReference, Object timezone) {

			Assert.hasText(fieldReference, "FieldReference must not be null!");
			return new Month(Fields.field(fieldReference), timezone);
		}

		/**
		 * Creates new {@link Month} in the UTC timezone.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Month monthOf(AggregationExpression expression) {
			return monthOf(expression, null);
		}

		/**
		 * Creates new {@link Month} in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param expression must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public static Month monthOf(AggregationExpression expression, Object timezone) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Month(expression, timezone);
		}

		/**
		 * Creates new {@link Month} for current date in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors. If Mongo is less than 3.6,
		 * timezone must be null.
		 *
		 * @param dateFactory must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed. (supports Mongo prior to 3.6 if null)
		 * @since 2.1
		 * @return
		 */
		public static Month monthOf(DateFactory dateFactory, Object timezone) {

			Assert.notNull(dateFactory, "dateFactory must not be null!");
			return new Month(dateFactory, timezone);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $week}. This behavior is the same as the “%U” operator to the strftime
	 *
	 * @author Christoph Strobl
	 * @see https://docs.mongodb.com/manual/reference/operator/aggregation/week/
	 */
	public static class Week extends DateAggregationExpression {

		private Week(Object value, Object timezone) {
			super(value, timezone);
		}

		@Override
		protected String getMongoMethod() {
			return "$week";
		}

		/**
		 * Creates new {@link Week} in the UTC timezone.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Week weekOf(String fieldReference) {
			return weekOf(fieldReference, null);
		}

		/**
		 * Creates new {@link Week} in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public static Week weekOf(String fieldReference, Object timezone) {

			Assert.hasText(fieldReference, "FieldReference must not be null!");
			return new Week(Fields.field(fieldReference), timezone);
		}

		/**
		 * Creates new {@link Week} in the UTC timezone.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Week weekOf(AggregationExpression expression) {
			return weekOf(expression, null);
		}

		/**
		 * Creates new {@link Week} in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param expression must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public static Week weekOf(AggregationExpression expression, Object timezone) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Week(expression, timezone);
		}

		/**
		 * Creates new {@link Week} for current date in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors. If Mongo is less than 3.6,
		 * timezone must be null.
		 *
		 * @param dateFactory must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed. (supports Mongo prior to 3.6 if null)
		 * @since 2.1
		 * @return
		 */
		public static Week weekOf(DateFactory dateFactory, Object timezone) {

			Assert.notNull(dateFactory, "dateFactory must not be null!");
			return new Week(dateFactory, timezone);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $hour}.
	 *
	 * @author Christoph Strobl
	 * @see https://docs.mongodb.com/manual/reference/operator/aggregation/hour/
	 */
	public static class Hour extends DateAggregationExpression {

		private Hour(Object value, Object timezone) {
			super(value, timezone);
		}

		@Override
		protected String getMongoMethod() {
			return "$hour";
		}

		/**
		 * Creates new {@link Hour} in the UTC timezone.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Hour hourOf(String fieldReference) {
			return hourOf(fieldReference, null);
		}

		/**
		 * Creates new {@link Hour} in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public static Hour hourOf(String fieldReference, Object timezone) {

			Assert.hasText(fieldReference, "FieldReference must not be null!");
			return new Hour(Fields.field(fieldReference), timezone);
		}

		/**
		 * Creates new {@link Hour} in the UTC timezone.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Hour hourOf(AggregationExpression expression) {
			return hourOf(expression, null);
		}

		/**
		 * Creates new {@link Hour} in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param expression must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public static Hour hourOf(AggregationExpression expression, Object timezone) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Hour(expression, timezone);
		}

		/**
		 * Creates new {@link Hour} for current date in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors. If Mongo is less than 3.6,
		 * timezone must be null.
		 *
		 * @param dateFactory must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed. (supports Mongo prior to 3.6 if null)
		 * @since 2.1
		 * @return
		 */
		public static Hour hourOf(DateFactory dateFactory, Object timezone) {

			Assert.notNull(dateFactory, "dateFactory must not be null!");
			return new Hour(dateFactory, timezone);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $minute}.
	 *
	 * @author Christoph Strobl
	 * @see https://docs.mongodb.com/manual/reference/operator/aggregation/minute/
	 */
	public static class Minute extends DateAggregationExpression {

		private Minute(Object value, Object timezone) {
			super(value, timezone);
		}

		@Override
		protected String getMongoMethod() {
			return "$minute";
		}

		/**
		 * Creates new {@link Minute} in the UTC timezone.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Minute minuteOf(String fieldReference) {
			return minuteOf(fieldReference, null);
		}

		/**
		 * Creates new {@link Minute} in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public static Minute minuteOf(String fieldReference, Object timezone) {

			Assert.hasText(fieldReference, "FieldReference must not be null!");
			return new Minute(Fields.field(fieldReference), timezone);
		}

		/**
		 * Creates new {@link Minute} in the UTC timezone.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Minute minuteOf(AggregationExpression expression) {
			return minuteOf(expression, null);
		}

		/**
		 * Creates new {@link Minute} in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param expression must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public static Minute minuteOf(AggregationExpression expression, Object timezone) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Minute(expression, timezone);
		}

		/**
		 * Creates new {@link Minute} for current date in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors. If Mongo is less than 3.6,
		 * timezone must be null.
		 *
		 * @param dateFactory must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed. (supports Mongo prior to 3.6 if null)
		 * @since 2.1
		 * @return
		 */
		public static Minute minuteOf(DateFactory dateFactory, Object timezone) {

			Assert.notNull(dateFactory, "dateFactory must not be null!");
			return new Minute(dateFactory, timezone);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $second}.
	 *
	 * @author Christoph Strobl
	 * @see https://docs.mongodb.com/manual/reference/operator/aggregation/second/
	 */
	public static class Second extends DateAggregationExpression {

		private Second(Object value, Object timezone) {
			super(value, timezone);
		}

		@Override
		protected String getMongoMethod() {
			return "$second";
		}

		/**
		 * Creates new {@link Second} in the UTC timezone.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Second secondOf(String fieldReference) {
			return secondOf(fieldReference, null);
		}

		/**
		 * Creates new {@link Second} in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public static Second secondOf(String fieldReference, Object timezone) {

			Assert.hasText(fieldReference, "FieldReference must not be null!");
			return new Second(Fields.field(fieldReference), timezone);
		}

		/**
		 * Creates new {@link Second} in the UTC timezone.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Second secondOf(AggregationExpression expression) {
			return secondOf(expression, null);
		}

		/**
		 * Creates new {@link Second} in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param expression must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public static Second secondOf(AggregationExpression expression, Object timezone) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Second(expression, timezone);
		}

		/**
		 * Creates new {@link Second} for current date in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors. If Mongo is less than 3.6,
		 * timezone must be null.
		 *
		 * @param dateFactory must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed. (supports Mongo prior to 3.6 if null)
		 * @since 2.1
		 * @return
		 */
		public static Second secondOf(DateFactory dateFactory, Object timezone) {

			Assert.notNull(dateFactory, "dateFactory must not be null!");
			return new Second(dateFactory, timezone);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $millisecond}.
	 *
	 * @author Christoph Strobl
	 * @see https://docs.mongodb.com/manual/reference/operator/aggregation/millisecond/
	 */
	public static class Millisecond extends DateAggregationExpression {

		private Millisecond(Object value, Object timezone) {
			super(value, timezone);
		}

		@Override
		protected String getMongoMethod() {
			return "$millisecond";
		}

		/**
		 * Creates new {@link Millisecond} in the UTC timezone.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Millisecond millisecondOf(String fieldReference) {
			return millisecondOf(fieldReference, null);
		}

		/**
		 * Creates new {@link Millisecond} in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public static Millisecond millisecondOf(String fieldReference, Object timezone) {

			Assert.hasText(fieldReference, "FieldReference must not be null!");
			return new Millisecond(Fields.field(fieldReference), timezone);
		}

		/**
		 * Creates new {@link Millisecond} in the UTC timezone.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Millisecond millisecondOf(AggregationExpression expression) {
			return millisecondOf(expression, null);
		}

		/**
		 * Creates new {@link Millisecond} in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param expression must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public static Millisecond millisecondOf(AggregationExpression expression, Object timezone) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Millisecond(expression, timezone);
		}

		/**
		 * Creates new {@link Millisecond} for current date in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors. If Mongo is less than 3.6,
		 * timezone must be null.
		 *
		 * @param dateFactory must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed. (supports Mongo prior to 3.6 if null)
		 * @since 2.1
		 * @return
		 */
		public static Millisecond millisecondOf(DateFactory dateFactory, Object timezone) {

			Assert.notNull(dateFactory, "dateFactory must not be null!");
			return new Millisecond(dateFactory, timezone);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $isoDayOfWeek}.
	 *
	 * @author Christoph Strobl
	 * @see https://docs.mongodb.com/manual/reference/operator/aggregation/isoDayOfWeek/
	 */
	public static class IsoDayOfWeek extends DateAggregationExpression {

		private IsoDayOfWeek(Object value, Object timezone) {
			super(value, timezone);
		}

		@Override
		protected String getMongoMethod() {
			return "$isoDayOfWeek";
		}

		/**
		 * Creates new {@link IsoDayOfWeek} in the UTC timezone.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static IsoDayOfWeek isoDayOfWeek(String fieldReference) {
			return isoDayOfWeek(fieldReference, null);
		}

		/**
		 * Creates new {@link IsoDayOfWeek} in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @since 2.1
		 * @param fieldReference must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed.
		 * @return
		 */
		public static IsoDayOfWeek isoDayOfWeek(String fieldReference, Object timezone) {

			Assert.hasText(fieldReference, "FieldReference must not be null!");
			return new IsoDayOfWeek(Fields.field(fieldReference), timezone);
		}

		/**
		 * Creates new {@link IsoDayOfWeek} in the UTC timezone.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static IsoDayOfWeek isoDayOfWeek(AggregationExpression expression) {
			return isoDayOfWeek(expression, null);
		}

		/**
		 * Creates new {@link IsoDayOfWeek} in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param expression must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public static IsoDayOfWeek isoDayOfWeek(AggregationExpression expression, Object timezone) {

			Assert.notNull(expression, "Expression must not be null!");
			return new IsoDayOfWeek(expression, timezone);
		}

		/**
		 * Creates new {@link IsoDayOfWeek} for current date in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors. If Mongo is less than 3.6,
		 * timezone must be null.
		 *
		 * @param dateFactory must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed. (supports Mongo prior to 3.6 if null)
		 * @since 2.1
		 * @return
		 */
		public static IsoDayOfWeek isoDayOfWeek(DateFactory dateFactory, Object timezone) {

			Assert.notNull(dateFactory, "dateFactory must not be null!");
			return new IsoDayOfWeek(dateFactory, timezone);
		}

	}

	/**
	 * {@link AggregationExpression} for {@code $isoWeek}.
	 *
	 * @author Christoph Strobl
	 * @see https://docs.mongodb.com/manual/reference/operator/aggregation/isoWeek/
	 */
	public static class IsoWeek extends DateAggregationExpression {

		private IsoWeek(Object value, Object timezone) {
			super(value, timezone);
		}

		@Override
		protected String getMongoMethod() {
			return "$isoWeek";
		}

		/**
		 * Creates new {@link IsoWeek} in the UTC timezone.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static IsoWeek isoWeekOf(String fieldReference) {
			return isoWeekOf(fieldReference, null);
		}

		/**
		 * Creates new {@link IsoWeek} in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public static IsoWeek isoWeekOf(String fieldReference, Object timezone) {

			Assert.hasText(fieldReference, "FieldReference must not be null!");
			return new IsoWeek(Fields.field(fieldReference), timezone);
		}

		/**
		 * Creates new {@link IsoWeek} in the UTC timezone.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static IsoWeek isoWeekOf(AggregationExpression expression) {
			return isoWeekOf(expression, null);
		}

		/**
		 * Creates new {@link IsoWeek} in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param expression must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public static IsoWeek isoWeekOf(AggregationExpression expression, Object timezone) {

			Assert.notNull(expression, "Expression must not be null!");
			return new IsoWeek(expression, timezone);
		}

		/**
		 * Creates new {@link IsoWeek} for current date in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors. If Mongo is less than 3.6,
		 * timezone must be null.
		 *
		 * @param dateFactory must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed. (supports Mongo prior to 3.6 if null)
		 * @since 2.1
		 * @return
		 */
		public static IsoWeek isoWeekOf(DateFactory dateFactory, Object timezone) {

			Assert.notNull(dateFactory, "dateFactory must not be null!");
			return new IsoWeek(dateFactory, timezone);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $isoWeekYear}.
	 *
	 * @author Christoph Strobl
	 * @see https://docs.mongodb.com/manual/reference/operator/aggregation/isoWeekYear/
	 */
	public static class IsoWeekYear extends DateAggregationExpression {

		private IsoWeekYear(Object value, Object timezone) {
			super(value, timezone);
		}

		@Override
		protected String getMongoMethod() {
			return "$isoWeekYear";
		}

		/**
		 * Creates new {@link IsoWeekYear} in the UTC timezone.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static IsoWeekYear isoWeekYearOf(String fieldReference) {
			return isoWeekYearOf(fieldReference, null);
		}

		/**
		 * Creates new {@link IsoWeekYear} in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public static IsoWeekYear isoWeekYearOf(String fieldReference, Object timezone) {

			Assert.hasText(fieldReference, "FieldReference must not be null!");
			return new IsoWeekYear(Fields.field(fieldReference), timezone);
		}

		/**
		 * Creates new {@link Millisecond} in the UTC timezone.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static IsoWeekYear isoWeekYearOf(AggregationExpression expression) {
			return isoWeekYearOf(expression, null);
		}

		/**
		 * Creates new {@link Millisecond} in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param expression must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public static IsoWeekYear isoWeekYearOf(AggregationExpression expression, Object timezone) {

			Assert.notNull(expression, "Expression must not be null!");
			return new IsoWeekYear(expression, timezone);
		}

		/**
		 * Creates new {@link IsoWeekYear} for current date in the given timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors. If Mongo is less than 3.6,
		 * timezone must be null.
		 *
		 * @param dateFactory must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed. (supports Mongo prior to 3.6 if null)
		 * @since 2.1
		 * @return
		 */
		public static IsoWeekYear isoWeekYearOf(DateFactory dateFactory, Object timezone) {

			Assert.notNull(dateFactory, "dateFactory must not be null!");
			return new IsoWeekYear(dateFactory, timezone);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $dateToString}.
	 *
	 * @author Christoph Strobl
	 * @see https://docs.mongodb.com/manual/reference/operator/aggregation/dateToString/
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
		 * Creates new {@link FormatBuilder} allowing to define the date format to apply in the UTC timezone
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static FormatBuilder dateOf(final String fieldReference) {
			return dateOf(fieldReference, null);
		}

		/**
		 * Creates new {@link FormatBuilder} allowing to define the date format to apply in the specified timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public static FormatBuilder dateOf(final String fieldReference, final Object timezone) {

			Assert.hasText(fieldReference, "FieldReference must not be null!");

			return new FormatBuilder() {

				@Override
				public DateToString toString(String format) {
					return toString(format, timezone);
				}

				@Override
				public DateToString toString(String format, Object timezone) {

					Assert.notNull(format, "Format must not be null!");
					return new DateToString(argumentMap(Fields.field(fieldReference), format, timezone));
				}
			};
		}

		/**
		 * Creates new {@link FormatBuilder} allowing to define the date format to apply in the UTC timezone.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static FormatBuilder dateOf(final AggregationExpression expression) {
			return dateOf(expression, null);
		}

		/**
		 * Creates new {@link FormatBuilder} allowing to define the date format to apply in the specified timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
		 *
		 * @param expression must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed.
		 * @since 2.1
		 * @return
		 */
		public static FormatBuilder dateOf(final AggregationExpression expression, final Object timezone) {

			Assert.notNull(expression, "Expression must not be null!");
			Assert.isTrue(DateAggregationExpression.isValidTimezoneObject(timezone),
					"Timezone was not a valid timezone: " + timezone + ". Must be String, AggregationExpression or Field");

			return new FormatBuilder() {

				@Override
				public DateToString toString(String format) {
					return toString(format, timezone);
				}

				@Override
				public DateToString toString(String format, Object timezone) {
					Assert.notNull(format, "Format must not be null!");
					return new DateToString(argumentMap(expression, format, timezone));
				}
			};
		}

		/**
		 * Creates new {@link FormatBuilder} allowing to define the date format to apply in the specified timezone.
		 * <p>
		 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors. If Mongo is less than 3.6,
		 * timezone must be null.
		 *
		 * @param dateFactory must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed. (supports Mongo prior to 3.6 if null)
		 * @since 2.1
		 * @return
		 */
		public static FormatBuilder dateOf(final DateFactory dateFactory, final Object timezone) {

			Assert.notNull(dateFactory, "CurrentDateFactory must not be null!");
			Assert.isTrue(DateAggregationExpression.isValidTimezoneObject(timezone),
					"Timezone was not a valid timezone: " + timezone + ". Must be String, AggregationExpression or Field");

			return new FormatBuilder() {

				@Override
				public DateToString toString(String format) {
					return toString(format, timezone);
				}

				@Override
				public DateToString toString(String format, Object timezone) {
					Assert.notNull(format, "Format must not be null!");
					return new DateToString(argumentMap(dateFactory, format, timezone));
				}
			};
		}

		private static java.util.Map<String, Object> argumentMap(Object date, String format, final Object timezone) {

			java.util.Map<String, Object> args = new LinkedHashMap<String, Object>(5);
			args.put("date", date);
			args.put("format", format);
			if (timezone != null) {
				Assert.isTrue(DateAggregationExpression.isValidTimezoneObject(timezone),
						"Timezone was not a valid timezone: " + timezone + ". Must be String, AggregationExpression or Field");
				args.put("timezone", timezone);
			}
			return args;
		}

		public interface FormatBuilder {

			/**
			 * Creates new {@link DateToString} with all previously added arguments appending the given one in the builder
			 * timezone (default UTC).
			 *
			 * @param format must not be {@literal null}.
			 * @return
			 */
			DateToString toString(String format);

			/**
			 * Creates new {@link DateToString} with all previously added arguments appending the given one in the given
			 * timezone.
			 * <p>
			 * WARNING: Mongo 3.6+ only. Using timezone on prior Mongo versions will cause errors.
			 *
			 * @param format must not be {@literal null}.
			 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression}
			 *          or {@link Field}. If null, UTC is assumed.
			 * @since 2.1
			 * @return
			 */
			DateToString toString(String format, Object timezone);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $dateFromString}.
	 * <p>
	 * WARNING: Mongo 3.6+ only.
	 *
	 * @since 2.1
	 * @author Matt Morrissette
	 * @see https://docs.mongodb.com/manual/reference/operator/aggregation/dateFromString/
	 */
	public static class DateFromString extends AbstractAggregationExpression {

		private DateFromString(Object value, Object timezone) {
			super(argumentMap(value, timezone));
		}

		private static Map<String, Object> argumentMap(final Object value, final Object timezone) {

			final Map<String, Object> vals = new LinkedHashMap<String, Object>(4);
			vals.put("dateString", value);
			if (timezone != null) {
				Assert.isTrue(DateAggregationExpression.isValidTimezoneObject(timezone),
						"Timezone was not a valid timezone: " + timezone + ". Must be String, AggregationExpression or Field");
				vals.put("timezone", timezone);
			}
			return vals;
		}

		@Override
		protected String getMongoMethod() {
			return "$dateFromString";
		}

		/**
		 * Creates new {@link DateFromString} in the UTC timezone for a date referencing a field.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static DateFromString dateFromString(final String fieldReference) {
			return dateFromString(fieldReference, null);
		}

		/**
		 * Creates new {@link DateFromString} in the given timezone for a date referencing a field.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed.
		 * @return
		 */
		public static DateFromString dateFromString(final String fieldReference, final Object timezone) {

			Assert.notNull(fieldReference, "fieldReference must not be null!");
			return new DateFromString(Fields.field(fieldReference), timezone);
		}

		/**
		 * Creates new {@link DateFromString} in the given timezone for a date from evaluating an AggregationExpression.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static DateFromString dateFromString(final AggregationExpression expression) {
			return dateFromString(expression, null);
		}

		/**
		 * Creates new {@link DateFromString} in the given timezone for a date from evaluating an AggregationExpression.
		 *
		 * @param expression must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed.
		 * @return
		 */
		public static DateFromString dateFromString(final AggregationExpression expression, final Object timezone) {

			Assert.notNull(expression, "expression must not be null!");
			return new DateFromString(expression, timezone);
		}

		/**
		 * Creates new {@link DateFromString} in the given timezone for a date provided by the given factory.
		 *
		 * @param factory must not be {@literal null}.
		 * @return
		 */
		public static DateFromString dateFromString(final DateFactory factory) {
			return dateFromString(factory, null);
		}

		/**
		 * Creates new {@link DateFromString} in the given timezone for a date provided by the given factory.
		 *
		 * @param factory must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed.
		 * @return
		 */
		public static DateFromString dateFromString(final DateFactory factory, final Object timezone) {

			Assert.notNull(factory, "factory must not be null!");
			return new DateFromString(factory, timezone);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $dateToParts}.
	 * <p>
	 * WARNING: Mongo 3.6+ only.
	 *
	 * @since 2.1
	 * @author Matt Morrissette
	 * @see https://docs.mongodb.com/manual/reference/operator/aggregation/dateToParts/
	 */
	public static class DateToParts extends AbstractAggregationExpression {

		private DateToParts(Object value, Object timezone, Boolean iso8601) {
			super(argumentMap(value, timezone, iso8601));
		}

		private static Map<String, Object> argumentMap(final Object value, Object timezone, Boolean iso8601) {

			final Map<String, Object> vals = new LinkedHashMap<String, Object>(6);
			vals.put("date", value);
			if (timezone != null) {
				if (timezone instanceof Boolean) {
					// iso8601 passed as second argument
					if (iso8601 == null) {
						iso8601 = (Boolean) timezone;
					} else {
						throw new IllegalArgumentException(
								"Timezone was not a valid timezone: " + timezone + ". Must be String, AggregationExpression or Field");
					}
				} else {
					Assert.isTrue(DateAggregationExpression.isValidTimezoneObject(timezone),
							"Timezone was not a valid timezone: " + timezone + ". Must be String, AggregationExpression or Field");
					vals.put("timezone", timezone);
				}
			}
			if (iso8601 != null) {
				vals.put("iso8601", iso8601);
			}
			return vals;
		}

		@Override
		protected String getMongoMethod() {
			return "$dateToParts";
		}

		/**
		 * Creates new {@link DateToParts} in the UTC timezone.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static DateToParts dateToParts(final String fieldReference) {
			return dateToParts(fieldReference, null, null);
		}

		/**
		 * Creates new {@link DateToParts} in the given timezone.
		 *
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed.
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static DateToParts dateToParts(final String fieldReference, final Object timezone) {
			return dateToParts(fieldReference, timezone, null);
		}

		/**
		 * Creates new {@link DateToParts} in the given timezone.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed.
		 * @param iso8601 If set to true, modifies the output document to use ISO week date fields. Defaults to false.
		 * @return
		 */
		public static DateToParts dateToParts(final String fieldReference, final Object timezone, final Boolean iso8601) {

			Assert.notNull(fieldReference, "fieldReference must not be null!");
			return new DateToParts(Fields.field(fieldReference), timezone, iso8601);
		}

		/**
		 * Creates new {@link DateToParts} in the UTC timezone.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static DateToParts dateToParts(final AggregationExpression expression) {
			return dateToParts(expression, null, null);
		}

		/**
		 * Creates new {@link DateToParts} in the given timezone.
		 *
		 * @param expression must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed.
		 * @return
		 */
		public static DateToParts dateToParts(final AggregationExpression expression, final Object timezone) {
			return dateToParts(expression, timezone, null);
		}

		/**
		 * Creates new {@link DateToParts} in the given timezone.
		 *
		 * @param expression must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed.
		 * @param iso8601 If set to true, modifies the output document to use ISO week date fields. Defaults to false.
		 * @return
		 */
		public static DateToParts dateToParts(final AggregationExpression expression, final Object timezone,
				final Boolean iso8601) {

			Assert.notNull(expression, "expression must not be null!");
			return new DateToParts(expression, timezone, iso8601);
		}

		/**
		 * Creates new {@link DateToParts} in the UTC timezone.
		 *
		 * @param factory must not be {@literal null}.
		 * @return
		 */
		public static DateToParts dateToParts(final DateFactory factory) {
			return dateToParts(factory, null, null);
		}

		/**
		 * Creates new {@link DateToParts} in the given timezone.
		 *
		 * @param factory must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed.
		 * @return
		 */
		public static DateToParts dateToParts(final DateFactory factory, final Object timezone) {
			return dateToParts(factory, timezone, null);
		}

		/**
		 * Creates new {@link DateToParts} in the given timezone.
		 *
		 * @param factory must not be {@literal null}.
		 * @param timezone nullable. The timezone ID or offset as a String. Also accepts a {@link AggregationExpression} or
		 *          {@link Field}. If null, UTC is assumed.
		 * @param iso8601 If set to true, modifies the output document to use ISO week date fields. Defaults to false.
		 * @return
		 */
		public static DateToParts dateToParts(final DateFactory factory, final Object timezone, final Boolean iso8601) {

			Assert.notNull(factory, "factory must not be null!");
			return new DateToParts(factory, timezone, iso8601);
		}

	}

	/**
	 * AggregationExpression for '$dateFromParts'
	 *
	 * @author matt.morrissette
	 * @see https://docs.mongodb.com/manual/reference/operator/aggregation/dateFromParts/
	 */
	public static class DateFromParts extends AbstractAggregationExpression {

		private DateFromParts(boolean isoWeek, Object yearOrIsoWeekYear, Object monthOrIsoWeek, Object dayOrIsoDayOfWeek,
				Object hour, Object minute, Object second, Object millisecond, Object timezone) {
			super(isoWeek
					? isoWeekMap(yearOrIsoWeekYear, monthOrIsoWeek, dayOrIsoDayOfWeek, hour, minute, second, millisecond,
							timezone)
					: calMap(yearOrIsoWeekYear, monthOrIsoWeek, dayOrIsoDayOfWeek, hour, minute, second, millisecond, timezone));
		}

		@Override
		protected String getMongoMethod() {
			return "$dateFromParts";
		}

		/**
		 * @return a new builder for {@link DateFromParts} using year/month/day
		 *         <p>
		 *         year is required
		 *         <p>
		 *         Timezone defaults to UTC if not specified
		 *         <p>
		 *         year and month default to 1 if not specified. All other fields default to 0.
		 */
		public static CalendarDatePartsBuilder fromParts() {
			return new CalendarDatePartsBuilder();
		}

		/**
		 * @return a new builder for {@link DateFromParts} using isoWeekYear/isoWeek/isoDayOfWeek
		 *         <p>
		 *         isoWeekYear is required
		 *         <p>
		 *         Timezone defaults to UTC if not specified
		 *         <p>
		 *         isoWeek and isoDayOfWeek default to 1 if not specified. All other fields default to 0.
		 */
		public static IsoWeekDatePartsBuilder fromIsoWeekParts() {
			return new IsoWeekDatePartsBuilder();
		}

		/**
		 * A Mutable builder to create a {@link DateFromParts} aggregation expression. All methods mutate this builder (they
		 * all return this for convenience)
		 *
		 * @param <Builder> The concrete builder (either {@link CalendarDatePartsBuilder} for calendar date (i.e.
		 *          year/month/day) or {@link IsoWeekDatePartsBuilder} for ISO week 8601 dates
		 *          (isoWeekYear/isoWeek/isoDayOfWeek)
		 */
		public abstract static class DatePartsBuilder<Builder extends DatePartsBuilder<Builder>> {

			protected Object hour;

			protected Object minute;

			protected Object second;

			protected Object millisecond;

			protected Object timezone;

			/**
			 * Sets the 'hour' of the {@link DateFromParts} to given the fixed value
			 *
			 * @param hour the fixed value to bind the field
			 * @return
			 */
			@SuppressWarnings("unchecked")
			public Builder hour(Number hour) {

				this.hour = hour;
				return (Builder) this;
			}

			/**
			 * Sets the 'hour' of the {@link DateFromParts} to given the field
			 *
			 * @param hour the field to read the 'hour' value from
			 * @return
			 */
			@SuppressWarnings("unchecked")
			public Builder hourOf(String hour) {

				this.hour = Fields.field(hour);
				return (Builder) this;
			}

			/**
			 * Sets the 'hour' of the {@link DateFromParts} to given the fixed value
			 *
			 * @param hour the expression to evaluate the 'hour' value
			 * @return
			 */
			@SuppressWarnings("unchecked")
			public Builder hourOf(AggregationExpression hour) {

				this.hour = hour;
				return (Builder) this;
			}

			/**
			 * Sets the 'minute' of the {@link DateFromParts} to given the fixed value
			 *
			 * @param minute the fixed value to bind the field
			 * @return
			 */
			@SuppressWarnings("unchecked")
			public Builder minute(Number minute) {

				this.minute = minute;
				return (Builder) this;
			}

			/**
			 * Sets the 'minute' of the {@link DateFromParts} to given the field
			 *
			 * @param minute the field to read the 'minute' value from
			 * @return
			 */
			@SuppressWarnings("unchecked")
			public Builder minuteOf(String minute) {

				this.minute = Fields.field(minute);
				return (Builder) this;
			}

			/**
			 * Sets the 'minute' of the {@link DateFromParts} to given the fixed value
			 *
			 * @param minute the expression to evaluate the 'minute' value
			 * @return
			 */
			@SuppressWarnings("unchecked")
			public Builder minuteOf(AggregationExpression minute) {

				this.minute = minute;
				return (Builder) this;
			}

			/**
			 * Sets the 'second' of the {@link DateFromParts} to given the fixed value
			 *
			 * @param second the fixed value to bind the field
			 * @return
			 */
			@SuppressWarnings("unchecked")
			public Builder second(Number second) {

				this.second = second;
				return (Builder) this;
			}

			/**
			 * Sets the 'second' of the {@link DateFromParts} to given the field
			 *
			 * @param second the field to read the 'second' value from
			 * @return
			 */
			@SuppressWarnings("unchecked")
			public Builder secondOf(String second) {

				this.second = Fields.field(second);
				return (Builder) this;
			}

			/**
			 * Sets the 'second' of the {@link DateFromParts} to given the fixed value
			 *
			 * @param second the expression to evaluate the 'second' value
			 * @return
			 */
			@SuppressWarnings("unchecked")
			public Builder secondOf(AggregationExpression second) {

				this.second = second;
				return (Builder) this;
			}

			/**
			 * Sets the 'millisecond' of the {@link DateFromParts} to given the fixed value
			 *
			 * @param millisecond the fixed value to bind the field
			 * @return
			 */
			@SuppressWarnings("unchecked")
			public Builder millisecond(Number millisecond) {

				this.millisecond = millisecond;
				return (Builder) this;
			}

			/**
			 * Sets the 'millisecond' of the {@link DateFromParts} to given the field
			 *
			 * @param millisecond the field to read the 'millisecond' value from
			 * @return
			 */
			@SuppressWarnings("unchecked")
			public Builder millisecondOf(String millisecond) {

				this.millisecond = Fields.field(millisecond);
				return (Builder) this;
			}

			/**
			 * Sets the 'millisecond' of the {@link DateFromParts} to given the fixed value
			 *
			 * @param millisecond the expression to evaluate the 'millisecond' value
			 * @return
			 */
			@SuppressWarnings("unchecked")
			public Builder millisecondOf(AggregationExpression millisecond) {

				this.millisecond = millisecond;
				return (Builder) this;
			}

			/**
			 * Sets the 'timezone' of the {@link DateFromParts} to given the fixed value
			 *
			 * @param timezone the fixed value to bind the field
			 * @return
			 */
			@SuppressWarnings("unchecked")
			public Builder timezone(String timezone) {

				this.timezone = timezone;
				return (Builder) this;
			}

			/**
			 * Sets the 'timezone' of the {@link DateFromParts} to given the field
			 *
			 * @param timezone the field to read the 'timezone' value from
			 * @return
			 */
			@SuppressWarnings("unchecked")
			public Builder timezoneOf(String timezone) {

				this.timezone = Fields.field(timezone);
				return (Builder) this;
			}

			/**
			 * Sets the 'timezone' of the {@link DateFromParts} to given the fixed value
			 *
			 * @param timezone the expression to evaluate the 'timezone' value
			 * @return
			 */
			@SuppressWarnings("unchecked")
			public Builder timezoneOf(AggregationExpression timezone) {

				this.timezone = timezone;
				return (Builder) this;
			}

			public abstract DateFromParts toDate();
		}

		public static class CalendarDatePartsBuilder extends DatePartsBuilder<CalendarDatePartsBuilder> {

			private Object year;

			private Object month;

			private Object day;

			@Override
			public DateFromParts toDate() {
				return new DateFromParts(false, year, month, day, hour, minute, second, millisecond, timezone);
			}

			/**
			 * Sets the 'year' of the {@link DateFromParts} to given the fixed value
			 *
			 * @param year the fixed value to bind the field
			 * @return
			 */
			public CalendarDatePartsBuilder year(Number year) {

				this.year = year;
				return this;
			}

			/**
			 * Sets the 'year' of the {@link DateFromParts} to given the field
			 *
			 * @param year the field to read the 'year' value from
			 * @return
			 */
			public CalendarDatePartsBuilder yearOf(String year) {

				this.year = Fields.field(year);
				return this;
			}

			/**
			 * Sets the 'year' of the {@link DateFromParts} to given the fixed value
			 *
			 * @param year the expression to evaluate the 'year' value
			 * @return
			 */
			public CalendarDatePartsBuilder yearOf(AggregationExpression year) {

				this.year = year;
				return this;
			}

			/**
			 * Sets the 'month' of the {@link DateFromParts} to given the fixed value
			 *
			 * @param month the fixed value to bind the field
			 * @return
			 */
			public CalendarDatePartsBuilder month(Number month) {

				this.month = month;
				return this;
			}

			/**
			 * Sets the 'month' of the {@link DateFromParts} to given the field
			 *
			 * @param month the field to read the 'month' value from
			 * @return
			 */
			public CalendarDatePartsBuilder monthOf(String month) {

				this.month = Fields.field(month);
				return this;
			}

			/**
			 * Sets the 'month' of the {@link DateFromParts} to given the fixed value
			 *
			 * @param month the expression to evaluate the 'month' value
			 * @return
			 */
			public CalendarDatePartsBuilder monthOf(AggregationExpression month) {

				this.month = month;
				return this;
			}

			/**
			 * Sets the 'day' of the {@link DateFromParts} to given the fixed value
			 *
			 * @param day the fixed value to bind the field
			 * @return
			 */
			public CalendarDatePartsBuilder day(Number day) {

				this.day = day;
				return this;
			}

			/**
			 * Sets the 'day' of the {@link DateFromParts} to given the field
			 *
			 * @param day the field to read the 'day' value from
			 * @return
			 */
			public CalendarDatePartsBuilder dayOf(String day) {

				this.day = Fields.field(day);
				return this;
			}

			/**
			 * Sets the 'day' of the {@link DateFromParts} to given the fixed value
			 *
			 * @param day the expression to evaluate the 'day' value
			 * @return
			 */
			public CalendarDatePartsBuilder dayOf(AggregationExpression day) {

				this.day = day;
				return this;
			}

		}

		public static class IsoWeekDatePartsBuilder extends DatePartsBuilder<IsoWeekDatePartsBuilder> {

			private Object isoWeekYear;

			private Object isoWeek;

			private Object isoDayOfWeek;

			@Override
			public DateFromParts toDate() {
				return new DateFromParts(true, isoWeekYear, isoWeek, isoDayOfWeek, hour, minute, second, millisecond, timezone);
			}

			/**
			 * Sets the 'isoWeekYear' of the {@link DateFromParts} to given the fixed value
			 *
			 * @param isoWeekYear the fixed value to bind the field
			 * @return
			 */
			public IsoWeekDatePartsBuilder isoWeekYear(Number isoWeekYear) {

				this.isoWeekYear = isoWeekYear;
				return this;
			}

			/**
			 * Sets the 'isoWeekYear' of the {@link DateFromParts} to given the field
			 *
			 * @param isoWeekYear the field to read the 'isoWeekYear' value from
			 * @return
			 */
			public IsoWeekDatePartsBuilder isoWeekYearOf(String isoWeekYear) {

				this.isoWeekYear = Fields.field(isoWeekYear);
				return this;
			}

			/**
			 * Sets the 'isoWeekYear' of the {@link DateFromParts} to given the fixed value
			 *
			 * @param isoWeekYear the expression to evaluate the 'isoWeekYear' value
			 * @return
			 */
			public IsoWeekDatePartsBuilder isoWeekYearOf(AggregationExpression isoWeekYear) {

				this.isoWeekYear = isoWeekYear;
				return this;
			}

			/**
			 * Sets the 'isoWeek' of the {@link DateFromParts} to given the fixed value
			 *
			 * @param isoWeek the fixed value to bind the field
			 * @return
			 */
			public IsoWeekDatePartsBuilder isoWeek(Number isoWeek) {

				this.isoWeek = isoWeek;
				return this;
			}

			/**
			 * Sets the 'isoWeek' of the {@link DateFromParts} to given the field
			 *
			 * @param isoWeek the field to read the 'isoWeek' value from
			 * @return
			 */
			public IsoWeekDatePartsBuilder isoWeekOf(String isoWeek) {

				this.isoWeek = Fields.field(isoWeek);
				return this;
			}

			/**
			 * Sets the 'isoWeek' of the {@link DateFromParts} to given the fixed value
			 *
			 * @param isoWeek the expression to evaluate the 'isoWeek' value
			 * @return
			 */
			public IsoWeekDatePartsBuilder isoWeekOf(AggregationExpression isoWeek) {

				this.isoWeek = isoWeek;
				return this;
			}

			/**
			 * Sets the 'isoDayOfWeek' of the {@link DateFromParts} to given the fixed value
			 *
			 * @param isoDayOfWeek the fixed value to bind the field
			 * @return
			 */
			public IsoWeekDatePartsBuilder isoDayOfWeek(Number isoDayOfWeek) {

				this.isoDayOfWeek = isoDayOfWeek;
				return this;
			}

			/**
			 * Sets the 'isoDayOfWeek' of the {@link DateFromParts} to given the field
			 *
			 * @param isoDayOfWeek the field to read the 'isoDayOfWeek' value from
			 * @return
			 */
			public IsoWeekDatePartsBuilder isoDayOfWeekOf(String isoDayOfWeek) {

				this.isoDayOfWeek = Fields.field(isoDayOfWeek);
				return this;
			}

			/**
			 * Sets the 'isoDayOfWeek' of the {@link DateFromParts} to given the fixed value
			 *
			 * @param isoDayOfWeek the expression to evaluate the 'isoDayOfWeek' value
			 * @return
			 */
			public IsoWeekDatePartsBuilder isoDayOfWeekOf(AggregationExpression isoDayOfWeek) {

				this.isoDayOfWeek = isoDayOfWeek;
				return this;
			}

		}

		private static Map<String, Object> calMap(Object year, Object month, Object day, Object hour, Object minute,
				Object second, Object millisecond, Object timezone) {

			final Map<String, Object> vals = new LinkedHashMap<String, Object>(11);
			put(vals, "year", year, true);
			put(vals, "month", month, false);
			put(vals, "day", day, false);
			putCommonMap(vals, hour, minute, second, millisecond, timezone);
			return vals;
		}

		private static Map<String, Object> isoWeekMap(Object isoWeekYear, Object isoWeek, Object isoDayOfWeek, Object hour,
				Object minute, Object second, Object millisecond, Object timezone) {

			final Map<String, Object> vals = new LinkedHashMap<String, Object>(11);
			put(vals, "isoWeekYear", isoWeekYear, true);
			put(vals, "isoWeek", isoWeek, false);
			put(vals, "isoDayOfWeek", isoDayOfWeek, false);
			putCommonMap(vals, hour, minute, second, millisecond, timezone);
			return vals;
		}

		private static void putCommonMap(final Map<String, Object> vals, Object hour, Object minute, Object second,
				Object millisecond, Object timezone) {

			put(vals, "hour", hour, false);
			put(vals, "minute", minute, false);
			put(vals, "second", second, false);
			put(vals, "millisecond", millisecond, false);
			if (timezone != null) {
				Assert.isTrue(DateAggregationExpression.isValidTimezoneObject(timezone),
						"Timezone was not a valid timezone: " + timezone + ". Must be String, AggregationExpression or Field");
				vals.put("timezone", timezone);
			}
		}

		private static void put(final Map<String, Object> map, final String key, final Object val, boolean throwIfAbsent) {

			if (val != null) {
				map.put(key, val);
			} else if (throwIfAbsent) {
				throw new IllegalArgumentException(key + "is required");
			}
		}
	}
}
