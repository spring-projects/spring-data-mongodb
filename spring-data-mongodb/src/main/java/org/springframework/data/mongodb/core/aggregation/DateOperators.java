/*
 * Copyright 2016-2023 the original author or authors.
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

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * Gateway to {@literal Date} aggregation operations.
 *
 * @author Christoph Strobl
 * @author Matt Morrissette
 * @since 1.10
 */
public class DateOperators {

	/**
	 * Take the date referenced by given {@literal fieldReference}.
	 *
	 * @param fieldReference must not be {@literal null}.
	 * @return new instance of {@link DateOperatorFactory}.
	 */
	public static DateOperatorFactory dateOf(String fieldReference) {

		Assert.notNull(fieldReference, "FieldReference must not be null");
		return new DateOperatorFactory(fieldReference);
	}

	/**
	 * Take the date referenced by given {@literal fieldReference}.
	 *
	 * @param fieldReference must not be {@literal null}.
	 * @return new instance of {@link DateOperatorFactory}.
	 * @since 3.3
	 */
	public static DateOperatorFactory zonedDateOf(String fieldReference, Timezone timezone) {

		Assert.notNull(fieldReference, "FieldReference must not be null");
		return new DateOperatorFactory(fieldReference).withTimezone(timezone);
	}

	/**
	 * Take the date resulting from the given {@link AggregationExpression}.
	 *
	 * @param expression must not be {@literal null}.
	 * @return new instance of {@link DateOperatorFactory}.
	 */
	public static DateOperatorFactory dateOf(AggregationExpression expression) {

		Assert.notNull(expression, "Expression must not be null");
		return new DateOperatorFactory(expression);
	}

	/**
	 * Take the date resulting from the given {@link AggregationExpression}.
	 *
	 * @param expression must not be {@literal null}.
	 * @return new instance of {@link DateOperatorFactory}.
	 * @since 3.3
	 */
	public static DateOperatorFactory zonedDateOf(AggregationExpression expression, Timezone timezone) {

		Assert.notNull(expression, "Expression must not be null");
		return new DateOperatorFactory(expression).withTimezone(timezone);
	}

	/**
	 * Take the given value as date. <br />
	 * This can be one of:
	 * <ul>
	 * <li>{@link java.util.Date}</li>
	 * <li>{@link java.util.Calendar}</li>
	 * <li>{@link java.time.Instant}</li>
	 * <li>{@link java.time.ZonedDateTime}</li>
	 * <li>{@link java.lang.Long}</li>
	 * <li>{@link Field}</li>
	 * <li>{@link AggregationExpression}</li>
	 * </ul>
	 *
	 * @param value must not be {@literal null}.
	 * @return new instance of {@link DateOperatorFactory}.
	 * @since 2.1
	 */
	public static DateOperatorFactory dateValue(Object value) {

		Assert.notNull(value, "Value must not be null");
		return new DateOperatorFactory(value);
	}

	/**
	 * Construct a Date object by providing the date’s constituent properties.<br />
	 * <strong>NOTE:</strong> Requires MongoDB 3.6 or later.
	 *
	 * @return new instance of {@link DateFromPartsOperatorFactory}.
	 * @since 2.1
	 */
	public static DateFromPartsOperatorFactory dateFromParts() {
		return new DateFromPartsOperatorFactory(Timezone.none());
	}

	/**
	 * Construct a Date object from the given date {@link String}.<br />
	 * To use a {@link Field field reference} or {@link AggregationExpression} as source of the date string consider
	 * {@link DateOperatorFactory#fromString()} or {@link DateFromString#fromStringOf(AggregationExpression)}.<br />
	 * <strong>NOTE:</strong> Requires MongoDB 3.6 or later.
	 *
	 * @return new instance of {@link DateFromPartsOperatorFactory}.
	 * @since 2.1
	 */
	public static DateFromString dateFromString(String value) {
		return DateFromString.fromString(value);
	}

	/**
	 * Timezone represents a MongoDB timezone abstraction which can be represented with a timezone ID or offset as a
	 * {@link String}. Also accepts a {@link AggregationExpression} or {@link Field} that resolves to a {@link String} of
	 * either Olson Timezone Identifier or a UTC Offset.<br />
	 * <table>
	 * <tr>
	 * <th>Format</th>
	 * <th>Example</th>
	 * </tr>
	 * <tr>
	 * <td>Olson Timezone Identifier</td>
	 * <td>"America/New_York"<br />
	 * "Europe/London"<br />
	 * "GMT"</td>
	 * </tr>
	 * <tr>
	 * <td>UTC Offset</td>
	 * <td>+/-[hh]:[mm], e.g. "+04:45"<br />
	 * -[hh][mm], e.g. "-0530"<br />
	 * +/-[hh], e.g. "+03"</td>
	 * </tr>
	 * </table>
	 * <strong>NOTE:</strong> Support for timezones in aggregations Requires MongoDB 3.6 or later.
	 *
	 * @author Christoph Strobl
	 * @author Mark Paluch
	 * @since 2.1
	 */
	public static class Timezone {

		private static final Timezone NONE = new Timezone(null);

		private final @Nullable Object value;

		private Timezone(@Nullable Object value) {
			this.value = value;
		}

		/**
		 * Return an empty {@link Timezone}.
		 *
		 * @return never {@literal null}.
		 */
		public static Timezone none() {
			return NONE;
		}

		/**
		 * Create a {@link Timezone} for the given value which must be a valid expression that resolves to a {@link String}
		 * representing an Olson Timezone Identifier or UTC Offset.
		 *
		 * @param value the plain timezone {@link String}, a {@link Field} holding the timezone or an
		 *          {@link AggregationExpression} resulting in the timezone.
		 * @return new instance of {@link Timezone}.
		 */
		public static Timezone valueOf(Object value) {

			Assert.notNull(value, "Value must not be null");
			return new Timezone(value);
		}

		/**
		 * Create a {@link Timezone} for the given {@link TimeZone} rendering the offset as UTC offset.
		 *
		 * @param timeZone {@link TimeZone} rendering the offset as UTC offset.
		 * @return new instance of {@link Timezone}.
		 * @since 3.3
		 */
		public static Timezone fromOffset(TimeZone timeZone) {

			Assert.notNull(timeZone, "TimeZone must not be null");

			return fromOffset(
					ZoneOffset.ofTotalSeconds(Math.toIntExact(TimeUnit.MILLISECONDS.toSeconds(timeZone.getRawOffset()))));
		}

		/**
		 * Create a {@link Timezone} for the given {@link ZoneOffset} rendering the offset as UTC offset.
		 *
		 * @param offset {@link ZoneOffset} rendering the offset as UTC offset.
		 * @return new instance of {@link Timezone}.
		 * @since 3.3
		 */
		public static Timezone fromOffset(ZoneOffset offset) {

			Assert.notNull(offset, "ZoneOffset must not be null");
			return new Timezone(offset.toString());
		}

		/**
		 * Create a {@link Timezone} for the given {@link TimeZone} rendering the offset as UTC offset.
		 *
		 * @param timeZone {@link Timezone} rendering the offset as zone identifier.
		 * @return new instance of {@link Timezone}.
		 * @since 3.3
		 */
		public static Timezone fromZone(TimeZone timeZone) {

			Assert.notNull(timeZone, "TimeZone must not be null");

			return valueOf(timeZone.getID());
		}

		/**
		 * Create a {@link Timezone} for the given {@link java.time.ZoneId} rendering the offset as UTC offset.
		 *
		 * @param zoneId {@link ZoneId} rendering the offset as zone identifier.
		 * @return new instance of {@link Timezone}.
		 * @since 3.3
		 */
		public static Timezone fromZone(ZoneId zoneId) {

			Assert.notNull(zoneId, "ZoneId must not be null");
			return new Timezone(zoneId.toString());
		}

		/**
		 * Create a {@link Timezone} for the {@link Field} reference holding the Olson Timezone Identifier or UTC Offset.
		 *
		 * @param fieldReference the {@link Field} holding the timezone.
		 * @return new instance of {@link Timezone}.
		 */
		public static Timezone ofField(String fieldReference) {
			return valueOf(Fields.field(fieldReference));
		}

		/**
		 * Create a {@link Timezone} for the {@link AggregationExpression} resulting in the Olson Timezone Identifier or UTC
		 * Offset.
		 *
		 * @param expression the {@link AggregationExpression} resulting in the timezone.
		 * @return new instance of {@link Timezone}.
		 */
		public static Timezone ofExpression(AggregationExpression expression) {
			return valueOf(expression);
		}

		@Nullable
		Object getValue() {
			return value;
		}
	}

	/**
	 * @author Christoph Strobl
	 * @author Matt Morrissette
	 */
	public static class DateOperatorFactory {

		private final @Nullable String fieldReference;
		private final @Nullable Object dateValue;
		private final @Nullable AggregationExpression expression;
		private final Timezone timezone;

		/**
		 * @param fieldReference
		 * @param expression
		 * @param value
		 * @param timezone
		 * @since 2.1
		 */
		private DateOperatorFactory(@Nullable String fieldReference, @Nullable AggregationExpression expression,
				@Nullable Object value, Timezone timezone) {

			this.fieldReference = fieldReference;
			this.expression = expression;
			this.dateValue = value;
			this.timezone = timezone;
		}

		/**
		 * Creates new {@link DateOperatorFactory} for given {@literal fieldReference}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 */
		public DateOperatorFactory(String fieldReference) {

			this(fieldReference, null, null, Timezone.none());

			Assert.notNull(fieldReference, "FieldReference must not be null");
		}

		/**
		 * Creates new {@link DateOperatorFactory} for given {@link AggregationExpression}.
		 *
		 * @param expression must not be {@literal null}.
		 */
		public DateOperatorFactory(AggregationExpression expression) {

			this(null, expression, null, Timezone.none());

			Assert.notNull(expression, "Expression must not be null");
		}

		/**
		 * Creates new {@link DateOperatorFactory} for given {@code value} that resolves to a Date. <br />
		 * <ul>
		 * <li>{@link java.util.Date}</li>
		 * <li>{@link java.util.Calendar}</li>
		 * <li>{@link java.time.Instant}</li>
		 * <li>{@link java.time.ZonedDateTime}</li>
		 * <li>{@link java.lang.Long}</li>
		 * </ul>
		 *
		 * @param value must not be {@literal null}.
		 * @since 2.1
		 */
		public DateOperatorFactory(Object value) {

			this(null, null, value, Timezone.none());

			Assert.notNull(value, "Value must not be null");
		}

		/**
		 * Create a new {@link DateOperatorFactory} bound to a given {@link Timezone}.<br />
		 * <strong>NOTE:</strong> Requires Mongo 3.6 or later.
		 *
		 * @param timezone must not be {@literal null}. Use {@link Timezone#none()} instead.
		 * @return new instance of {@link DateOperatorFactory}.
		 * @throws IllegalArgumentException if given {@literal timezone} is {@literal null}.
		 * @since 2.1
		 */
		public DateOperatorFactory withTimezone(Timezone timezone) {

			Assert.notNull(timezone, "Timezone must not be null");
			return new DateOperatorFactory(fieldReference, expression, dateValue, timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that adds the value of the given {@link AggregationExpression
		 * expression} (in {@literal units}).
		 *
		 * @param expression must not be {@literal null}.
		 * @param unit the unit of measure. Must not be {@literal null}.
		 * @return new instance of {@link DateAdd}. @since 3.3
		 */
		public DateAdd addValueOf(AggregationExpression expression, String unit) {
			return applyTimezone(DateAdd.addValueOf(expression, unit).toDate(dateReference()), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that adds the value of the given {@link AggregationExpression
		 * expression} (in {@literal units}).
		 *
		 * @param expression must not be {@literal null}.
		 * @param unit the unit of measure. Must not be {@literal null}.
		 * @return new instance of {@link DateAdd}. @since 3.3
		 */
		public DateAdd addValueOf(AggregationExpression expression, TemporalUnit unit) {

			Assert.notNull(unit, "TemporalUnit must not be null");
			return applyTimezone(DateAdd.addValueOf(expression, unit.name().toLowerCase(Locale.ROOT)).toDate(dateReference()),
					timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that adds the value stored at the given {@literal field} (in
		 * {@literal units}).
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @param unit the unit of measure. Must not be {@literal null}.
		 * @return new instance of {@link DateAdd}. @since 3.3
		 */
		public DateAdd addValueOf(String fieldReference, String unit) {
			return applyTimezone(DateAdd.addValueOf(fieldReference, unit).toDate(dateReference()), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that adds the value stored at the given {@literal field} (in
		 * {@literal units}).
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @param unit the unit of measure. Must not be {@literal null}.
		 * @return new instance of {@link DateAdd}. @since 3.3
		 */
		public DateAdd addValueOf(String fieldReference, TemporalUnit unit) {

			Assert.notNull(unit, "TemporalUnit must not be null");

			return applyTimezone(
					DateAdd.addValueOf(fieldReference, unit.name().toLowerCase(Locale.ROOT)).toDate(dateReference()), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that adds the given value (in {@literal units}).
		 *
		 * @param value must not be {@literal null}.
		 * @param unit the unit of measure. Must not be {@literal null}.
		 * @return
		 * @since 3.3 new instance of {@link DateAdd}.
		 */
		public DateAdd add(Object value, String unit) {
			return applyTimezone(DateAdd.addValue(value, unit).toDate(dateReference()), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that adds the given value (in {@literal units}).
		 *
		 * @param value must not be {@literal null}.
		 * @param unit the unit of measure. Must not be {@literal null}.
		 * @return
		 * @since 3.3 new instance of {@link DateAdd}.
		 */
		public DateAdd add(Object value, TemporalUnit unit) {

			Assert.notNull(unit, "TemporalUnit must not be null");

			return applyTimezone(DateAdd.addValue(value, unit.name().toLowerCase(Locale.ROOT)).toDate(dateReference()),
					timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that subtracts the value of the given {@link AggregationExpression
		 * expression} (in {@literal units}).
		 *
		 * @param expression must not be {@literal null}.
		 * @param unit the unit of measure. Must not be {@literal null}.
		 * @return new instance of {@link DateSubtract}.
		 * @since 4.0
		 */
		public DateSubtract subtractValueOf(AggregationExpression expression, String unit) {
			return applyTimezone(DateSubtract.subtractValueOf(expression, unit).fromDate(dateReference()), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that subtracts the value of the given {@link AggregationExpression
		 * expression} (in {@literal units}).
		 *
		 * @param expression must not be {@literal null}.
		 * @param unit the unit of measure. Must not be {@literal null}.
		 * @return new instance of {@link DateSubtract}.
		 * @since 4.0
		 */
		public DateSubtract subtractValueOf(AggregationExpression expression, TemporalUnit unit) {

			Assert.notNull(unit, "TemporalUnit must not be null");
			return applyTimezone(
					DateSubtract.subtractValueOf(expression, unit.name().toLowerCase(Locale.ROOT)).fromDate(dateReference()),
					timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that subtracts the value stored at the given {@literal field} (in
		 * {@literal units}).
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @param unit the unit of measure. Must not be {@literal null}.
		 * @return new instance of {@link DateSubtract}.
		 * @since 4.0
		 */
		public DateSubtract subtractValueOf(String fieldReference, String unit) {
			return applyTimezone(DateSubtract.subtractValueOf(fieldReference, unit).fromDate(dateReference()), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that subtracts the value stored at the given {@literal field} (in
		 * {@literal units}).
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @param unit the unit of measure. Must not be {@literal null}.
		 * @return new instance of {@link DateSubtract}.
		 * @since 4.0
		 */
		public DateSubtract subtractValueOf(String fieldReference, TemporalUnit unit) {

			Assert.notNull(unit, "TemporalUnit must not be null");

			return applyTimezone(
					DateSubtract.subtractValueOf(fieldReference, unit.name().toLowerCase(Locale.ROOT)).fromDate(dateReference()),
					timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that subtracts the given value (in {@literal units}).
		 *
		 * @param value must not be {@literal null}.
		 * @param unit the unit of measure. Must not be {@literal null}.
		 * @return new instance of {@link DateSubtract}.
		 * @since 4.0
		 */
		public DateSubtract subtract(Object value, String unit) {
			return applyTimezone(DateSubtract.subtractValue(value, unit).fromDate(dateReference()), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that subtracts the given value (in {@literal units}).
		 *
		 * @param value must not be {@literal null}.
		 * @param unit the unit of measure. Must not be {@literal null}.
		 * @return new instance of {@link DateSubtract}.
		 * @since 4.0
		 */
		public DateSubtract subtract(Object value, TemporalUnit unit) {

			Assert.notNull(unit, "TemporalUnit must not be null");

			return applyTimezone(
					DateSubtract.subtractValue(value, unit.name().toLowerCase(Locale.ROOT)).fromDate(dateReference()), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that truncates a date to the given {@literal unit}.
		 *
		 * @param unit the unit of measure. Must not be {@literal null}.
		 * @return new instance of {@link DateTrunc}.
		 * @since 4.0
		 */
		public DateTrunc truncate(String unit) {

			Assert.notNull(unit, "TemporalUnit must not be null");
			return applyTimezone(DateTrunc.truncateValue(dateReference()).to(unit), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that truncates a date to the given {@literal unit}.
		 *
		 * @param unit the unit of measure. Must not be {@literal null}.
		 * @return new instance of {@link DateTrunc}.
		 * @since 4.0
		 */
		public DateTrunc truncate(TemporalUnit unit) {

			Assert.notNull(unit, "TemporalUnit must not be null");
			return truncate(unit.name().toLowerCase(Locale.ROOT));
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the day of the year for a date as a number between 1 and
		 * 366.
		 *
		 * @return new instance of {@link DayOfYear}.
		 */
		public DayOfYear dayOfYear() {
			return applyTimezone(DayOfYear.dayOfYear(dateReference()), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the day of the month for a date as a number between 1 and
		 * 31.
		 *
		 * @return new instance of {@link DayOfMonth}.
		 */
		public DayOfMonth dayOfMonth() {
			return applyTimezone(DayOfMonth.dayOfMonth(dateReference()), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the day of the week for a date as a number between 1
		 * (Sunday) and 7 (Saturday).
		 *
		 * @return new instance of {@link DayOfWeek}.
		 */
		public DayOfWeek dayOfWeek() {
			return applyTimezone(DayOfWeek.dayOfWeek(dateReference()), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the difference (in {@literal units}) to the date
		 * computed by the given {@link AggregationExpression expression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @param unit the unit of measure. Must not be {@literal null}.
		 * @return new instance of {@link DateAdd}. @since 3.3
		 */
		public DateDiff diffValueOf(AggregationExpression expression, String unit) {
			return applyTimezone(DateDiff.diffValueOf(expression, unit).toDate(dateReference()), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the difference (in {@literal units}) to the date
		 * computed by the given {@link AggregationExpression expression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @param unit the unit of measure. Must not be {@literal null}.
		 * @return new instance of {@link DateAdd}. @since 3.3
		 */
		public DateDiff diffValueOf(AggregationExpression expression, TemporalUnit unit) {

			Assert.notNull(unit, "TemporalUnit must not be null");

			return applyTimezone(
					DateDiff.diffValueOf(expression, unit.name().toLowerCase(Locale.ROOT)).toDate(dateReference()), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the difference (in {@literal units}) to the date stored
		 * at the given {@literal field}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @param unit the unit of measure. Must not be {@literal null}.
		 * @return new instance of {@link DateAdd}. @since 3.3
		 */
		public DateDiff diffValueOf(String fieldReference, String unit) {
			return applyTimezone(DateDiff.diffValueOf(fieldReference, unit).toDate(dateReference()), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the difference (in {@literal units}) to the date stored
		 * at the given {@literal field}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @param unit the unit of measure. Must not be {@literal null}.
		 * @return new instance of {@link DateAdd}. @since 3.3
		 */
		public DateDiff diffValueOf(String fieldReference, TemporalUnit unit) {

			Assert.notNull(unit, "TemporalUnit must not be null");

			return applyTimezone(
					DateDiff.diffValueOf(fieldReference, unit.name().toLowerCase(Locale.ROOT)).toDate(dateReference()), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the difference (in {@literal units}) to the date given
		 * {@literal value}.
		 *
		 * @param value anything the resolves to a valid date. Must not be {@literal null}.
		 * @param unit the unit of measure. Must not be {@literal null}.
		 * @return new instance of {@link DateAdd}. @since 3.3
		 */
		public DateDiff diff(Object value, String unit) {
			return applyTimezone(DateDiff.diffValue(value, unit).toDate(dateReference()), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that calculates the difference (in {@literal units}) to the date given
		 * {@literal value}.
		 *
		 * @param value anything the resolves to a valid date. Must not be {@literal null}.
		 * @param unit the unit of measure. Must not be {@literal null}.
		 * @return new instance of {@link DateAdd}. @since 3.3
		 */
		public DateDiff diff(Object value, TemporalUnit unit) {

			Assert.notNull(unit, "TemporalUnit must not be null");

			return applyTimezone(DateDiff.diffValue(value, unit.name().toLowerCase(Locale.ROOT)).toDate(dateReference()),
					timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the year portion of a date.
		 *
		 * @return new instance of {@link Year}.
		 */
		public Year year() {
			return applyTimezone(Year.year(dateReference()), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the month of a date as a number between 1 and 12.
		 *
		 * @return new instance of {@link Month}.
		 */
		public Month month() {
			return applyTimezone(Month.month(dateReference()), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the week of the year for a date as a number between 0 and
		 * 53.
		 *
		 * @return new instance of {@link Week}.
		 */
		public Week week() {
			return applyTimezone(Week.week(dateReference()), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the hour portion of a date as a number between 0 and 23.
		 *
		 * @return new instance of {@link Hour}.
		 */
		public Hour hour() {
			return applyTimezone(Hour.hour(dateReference()), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the minute portion of a date as a number between 0 and 59.
		 *
		 * @return new instance of {@link Minute}.
		 */
		public Minute minute() {
			return applyTimezone(Minute.minute(dateReference()), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the second portion of a date as a number between 0 and 59,
		 * but can be 60 to account for leap seconds.
		 *
		 * @return new instance of {@link Second}.
		 */
		public Second second() {
			return applyTimezone(Second.second(dateReference()), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the millisecond portion of a date as an integer between 0
		 * and 999.
		 *
		 * @return new instance of {@link Millisecond}.
		 */
		public Millisecond millisecond() {
			return applyTimezone(Millisecond.millisecond(dateReference()), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that converts a date object to a string according to a user-specified
		 * {@literal format}.
		 *
		 * @param format must not be {@literal null}.
		 * @return new instance of {@link DateToString}.
		 */
		public DateToString toString(String format) {
			return applyTimezone(DateToString.dateToString(dateReference()).toString(format), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that converts a date object to a string according to the server default
		 * format.
		 *
		 * @return new instance of {@link DateToString}.
		 * @since 2.1
		 */
		public DateToString toStringWithDefaultFormat() {
			return applyTimezone(DateToString.dateToString(dateReference()).defaultFormat(), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the weekday number in ISO 8601-2018 format, ranging from 1
		 * (for Monday) to 7 (for Sunday).
		 *
		 * @return new instance of {@link IsoDayOfWeek}.
		 */
		public IsoDayOfWeek isoDayOfWeek() {
			return applyTimezone(IsoDayOfWeek.isoDayWeek(dateReference()), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the week number in ISO 8601-2018 format, ranging from 1 to
		 * 53.
		 *
		 * @return new instance of {@link IsoWeek}.
		 */
		public IsoWeek isoWeek() {
			return applyTimezone(IsoWeek.isoWeek(dateReference()), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the year number in ISO 8601-2018 format.
		 *
		 * @return new instance of {@link IsoWeekYear}.
		 */
		public IsoWeekYear isoWeekYear() {
			return applyTimezone(IsoWeekYear.isoWeekYear(dateReference()), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns a document containing the constituent parts of the date as
		 * individual properties.<br />
		 * <strong>NOTE:</strong> Requires MongoDB 3.6 or later.
		 *
		 * @return new instance of {@link DateToParts}.
		 * @since 2.1
		 */
		public DateToParts toParts() {
			return applyTimezone(DateToParts.dateToParts(dateReference()), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that converts a date/time string to a date object.<br />
		 * <strong>NOTE:</strong> Requires MongoDB 3.6 or later.
		 *
		 * @return new instance of {@link DateFromString}.
		 * @since 2.1
		 */
		public DateFromString fromString() {
			return applyTimezone(DateFromString.fromString(dateReference()), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the incrementing ordinal from a timestamp.
		 *
		 * @return new instance of {@link TsIncrement}.
		 * @since 4.0
		 */
		public TsIncrement tsIncrement() {

			if (timezone != null && !Timezone.none().equals(timezone)) {
				throw new IllegalArgumentException("$tsIncrement does not support timezones");
			}

			return TsIncrement.tsIncrement(dateReference());
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the seconds from a timestamp.
		 *
		 * @return new instance of {@link TsIncrement}.
		 * @since 4.0
		 */
		public TsSecond tsSecond() {

			if (timezone != null && !Timezone.none().equals(timezone)) {
				throw new IllegalArgumentException("$tsSecond does not support timezones");
			}

			return TsSecond.tsSecond(dateReference());
		}

		private Object dateReference() {

			if (usesFieldRef()) {
				return Fields.field(fieldReference);
			}

			return usesExpression() ? expression : dateValue;
		}

		private boolean usesFieldRef() {
			return fieldReference != null;
		}

		private boolean usesExpression() {
			return expression != null;
		}
	}

	/**
	 * @author Matt Morrissette
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	public static class DateFromPartsOperatorFactory {

		private final Timezone timezone;

		private DateFromPartsOperatorFactory(Timezone timezone) {
			this.timezone = timezone;
		}

		/**
		 * Set the {@literal week date year} to the given value which must resolve to a weekday in range {@code 0 - 9999}.
		 * Can be a simple value, {@link Field field reference} or {@link AggregationExpression expression}.
		 *
		 * @param isoWeekYear must not be {@literal null}.
		 * @return new instance of {@link IsoDateFromParts} with {@link Timezone} if set.
		 * @throws IllegalArgumentException if given {@literal isoWeekYear} is {@literal null}.
		 */
		public IsoDateFromParts isoWeekYear(Object isoWeekYear) {
			return applyTimezone(IsoDateFromParts.dateFromParts().isoWeekYear(isoWeekYear), timezone);
		}

		/**
		 * Set the {@literal week date year} to the value resolved by following the given {@link Field field reference}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link IsoDateFromParts} with {@link Timezone} if set.
		 * @throws IllegalArgumentException if given {@literal fieldReference} is {@literal null}.
		 */
		public IsoDateFromParts isoWeekYearOf(String fieldReference) {
			return isoWeekYear(Fields.field(fieldReference));
		}

		/**
		 * Set the {@literal week date year} to the result of the given {@link AggregationExpression expression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link IsoDateFromParts} with {@link Timezone} if set.
		 * @throws IllegalArgumentException if given {@literal expression} is {@literal null}.
		 */
		public IsoDateFromParts isoWeekYearOf(AggregationExpression expression) {
			return isoWeekYear(expression);
		}

		/**
		 * Set the {@literal year} to the given value which must resolve to a calendar year. Can be a simple value,
		 * {@link Field field reference} or {@link AggregationExpression expression}.
		 *
		 * @param year must not be {@literal null}.
		 * @return new instance of {@link DateFromParts} with {@link Timezone} if set.
		 * @throws IllegalArgumentException if given {@literal year} is {@literal null}
		 */
		public DateFromParts year(Object year) {
			return applyTimezone(DateFromParts.dateFromParts().year(year), timezone);
		}

		/**
		 * Set the {@literal year} to the value resolved by following the given {@link Field field reference}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link DateFromParts} with {@link Timezone} if set.
		 * @throws IllegalArgumentException if given {@literal fieldReference} is {@literal null}.
		 */
		public DateFromParts yearOf(String fieldReference) {
			return year(Fields.field(fieldReference));
		}

		/**
		 * Set the {@literal year} to the result of the given {@link AggregationExpression expression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link DateFromParts} with {@link Timezone} if set.
		 * @throws IllegalArgumentException if given {@literal expression} is {@literal null}.
		 */
		public DateFromParts yearOf(AggregationExpression expression) {
			return year(expression);
		}

		/**
		 * Create a new {@link DateFromPartsOperatorFactory} bound to a given {@link Timezone}.<br />
		 *
		 * @param timezone must not be {@literal null}. Use {@link Timezone#none()} instead.
		 * @return new instance of {@link DateFromPartsOperatorFactory}.
		 * @throws IllegalArgumentException if given {@literal timezone} is {@literal null}.
		 */
		public DateFromPartsOperatorFactory withTimezone(Timezone timezone) {

			Assert.notNull(timezone, "Timezone must not be null");
			return new DateFromPartsOperatorFactory(timezone);
		}
	}

	/**
	 * {@link AggregationExpression} capable of setting a given {@link Timezone}.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	public static abstract class TimezonedDateAggregationExpression extends AbstractAggregationExpression {

		protected TimezonedDateAggregationExpression(Object value) {
			super(value);
		}

		/**
		 * Append the {@code timezone} to a given source. The source itself can be a {@link Map} of already set properties
		 * or a single value. In case of single value {@code source} the value will be added as {@code date} property.
		 *
		 * @param source must not be {@literal null}.
		 * @param timezone must not be {@literal null} use {@link Timezone#none()} instead.
		 * @return
		 */
		protected static java.util.Map<String, Object> appendTimezone(Object source, Timezone timezone) {

			java.util.Map<String, Object> args;

			if (source instanceof Map map) {
				args = new LinkedHashMap<>(map);
			} else {
				args = new LinkedHashMap<>(2);
				args.put("date", source);
			}

			if (!ObjectUtils.nullSafeEquals(Timezone.none(), timezone)) {
				args.put("timezone", timezone.value);
			} else if (args.containsKey("timezone")) {
				args.remove("timezone");
			}

			return args;
		}

		/**
		 * Optionally set the {@link Timezone} to use. If not specified {@literal UTC} is used.<br />
		 * <strong>NOTE:</strong> Requires MongoDB 3.6 or later.
		 *
		 * @param timezone must not be {@literal null}. Consider {@link Timezone#none()} instead.
		 * @return new instance.
		 * @throws IllegalArgumentException if given {@literal timezone} is {@literal null}.
		 */
		protected abstract TimezonedDateAggregationExpression withTimezone(Timezone timezone);

		protected boolean hasTimezone() {
			return contains("timezone");
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $dayOfYear}.
	 *
	 * @author Christoph Strobl
	 * @author Matt Morrissette
	 */
	public static class DayOfYear extends TimezonedDateAggregationExpression {

		private DayOfYear(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link DayOfYear}.
		 *
		 * @param value must not be {@literal null} and resolve to field, expression or object that represents a date.
		 * @return new instance of {@link DayOfYear}.
		 * @throws IllegalArgumentException if given value is {@literal null}.
		 * @since 2.1
		 */
		public static DayOfYear dayOfYear(Object value) {

			Assert.notNull(value, "value must not be null");
			return new DayOfYear(value);
		}

		/**
		 * Creates new {@link DayOfYear}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link DayOfYear}.
		 */
		public static DayOfYear dayOfYear(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return dayOfYear(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link DayOfYear}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link DayOfYear}.
		 */
		public static DayOfYear dayOfYear(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return dayOfYear((Object) expression);
		}

		/**
		 * Optionally set the {@link Timezone} to use. If not specified {@literal UTC} is used.<br />
		 * <strong>NOTE:</strong> Requires MongoDB 3.6 or later.
		 *
		 * @param timezone must not be {@literal null}. Consider {@link Timezone#none()} instead.
		 * @return new instance of {@link DayOfYear}.
		 * @throws IllegalArgumentException if given {@literal timezone} is {@literal null}.
		 * @since 2.1
		 */
		@Override
		public DayOfYear withTimezone(Timezone timezone) {

			Assert.notNull(timezone, "Timezone must not be null");
			return new DayOfYear(appendTimezone(values().iterator().next(), timezone));
		}

		@Override
		protected String getMongoMethod() {
			return "$dayOfYear";
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $dayOfMonth}.
	 *
	 * @author Christoph Strobl
	 * @author Matt Morrissette
	 */
	public static class DayOfMonth extends TimezonedDateAggregationExpression {

		private DayOfMonth(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link DayOfMonth}.
		 *
		 * @param value must not be {@literal null} and resolve to field, expression or object that represents a date.
		 * @return new instance of {@link DayOfMonth}.
		 * @throws IllegalArgumentException if given value is {@literal null}.
		 * @since 2.1
		 */
		public static DayOfMonth dayOfMonth(Object value) {

			Assert.notNull(value, "value must not be null");
			return new DayOfMonth(value);
		}

		/**
		 * Creates new {@link DayOfMonth}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link DayOfMonth}.
		 */
		public static DayOfMonth dayOfMonth(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return dayOfMonth(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link DayOfMonth}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link DayOfMonth}.
		 */
		public static DayOfMonth dayOfMonth(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return dayOfMonth((Object) expression);
		}

		/**
		 * Optionally set the {@link Timezone} to use. If not specified {@literal UTC} is used.<br />
		 * <strong>NOTE:</strong> Requires MongoDB 3.6 or later.
		 *
		 * @param timezone must not be {@literal null}. Consider {@link Timezone#none()} instead.
		 * @return new instance of {@link DayOfMonth}.
		 * @throws IllegalArgumentException if given {@literal timezone} is {@literal null}.
		 * @since 2.1
		 */
		@Override
		public DayOfMonth withTimezone(Timezone timezone) {

			Assert.notNull(timezone, "Timezone must not be null");
			return new DayOfMonth(appendTimezone(values().iterator().next(), timezone));
		}

		@Override
		protected String getMongoMethod() {
			return "$dayOfMonth";
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $dayOfWeek}.
	 *
	 * @author Christoph Strobl
	 * @author Matt Morrissette
	 */
	public static class DayOfWeek extends TimezonedDateAggregationExpression {

		private DayOfWeek(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link DayOfWeek}.
		 *
		 * @param value must not be {@literal null} and resolve to field, expression or object that represents a date.
		 * @return new instance of {@link DayOfWeek}.
		 * @throws IllegalArgumentException if given value is {@literal null}.
		 * @since 2.1
		 */
		public static DayOfWeek dayOfWeek(Object value) {

			Assert.notNull(value, "value must not be null");
			return new DayOfWeek(value);
		}

		/**
		 * Creates new {@link DayOfWeek}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link DayOfWeek}.
		 */
		public static DayOfWeek dayOfWeek(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return dayOfWeek(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link DayOfWeek}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link DayOfWeek}.
		 */
		public static DayOfWeek dayOfWeek(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return dayOfWeek((Object) expression);
		}

		/**
		 * Optionally set the {@link Timezone} to use. If not specified {@literal UTC} is used.<br />
		 * <strong>NOTE:</strong> Requires MongoDB 3.6 or later.
		 *
		 * @param timezone must not be {@literal null}. Consider {@link Timezone#none()} instead.
		 * @return new instance of {@link DayOfWeek}.
		 * @throws IllegalArgumentException if given {@literal timezone} is {@literal null}.
		 * @since 2.1
		 */
		@Override
		public DayOfWeek withTimezone(Timezone timezone) {

			Assert.notNull(timezone, "Timezone must not be null");
			return new DayOfWeek(appendTimezone(values().iterator().next(), timezone));
		}

		@Override
		protected String getMongoMethod() {
			return "$dayOfWeek";
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $year}.
	 *
	 * @author Christoph Strobl
	 * @author Matt Morrissette
	 */
	public static class Year extends TimezonedDateAggregationExpression {

		private Year(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link Year}.
		 *
		 * @param value must not be {@literal null} and resolve to field, expression or object that represents a date.
		 * @return new instance of {@link Year}.
		 * @throws IllegalArgumentException if given value is {@literal null}.
		 * @since 2.1
		 */
		public static Year year(Object value) {

			Assert.notNull(value, "value must not be null");
			return new Year(value);
		}

		/**
		 * Creates new {@link Year}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Year}.
		 */
		public static Year yearOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return year(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Year}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Year}.
		 */
		public static Year yearOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return year(expression);
		}

		/**
		 * Optionally set the {@link Timezone} to use. If not specified {@literal UTC} is used.<br />
		 * <strong>NOTE:</strong> Requires MongoDB 3.6 or later.
		 *
		 * @param timezone must not be {@literal null}. Consider {@link Timezone#none()} instead.
		 * @return new instance of {@link Year}.
		 * @throws IllegalArgumentException if given {@literal timezone} is {@literal null}.
		 * @since 2.1
		 */
		@Override
		public Year withTimezone(Timezone timezone) {

			Assert.notNull(timezone, "Timezone must not be null");
			return new Year(appendTimezone(values().iterator().next(), timezone));
		}

		@Override
		protected String getMongoMethod() {
			return "$year";
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $month}.
	 *
	 * @author Christoph Strobl
	 * @author Matt Morrissette
	 */
	public static class Month extends TimezonedDateAggregationExpression {

		private Month(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link Month}.
		 *
		 * @param value must not be {@literal null} and resolve to field, expression or object that represents a date.
		 * @return new instance of {@link Month}.
		 * @throws IllegalArgumentException if given value is {@literal null}.
		 * @since 2.1
		 */
		public static Month month(Object value) {

			Assert.notNull(value, "value must not be null");
			return new Month(value);
		}

		/**
		 * Creates new {@link Month}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Month}.
		 */
		public static Month monthOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return month(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Month}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Month}.
		 */
		public static Month monthOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return month(expression);
		}

		/**
		 * Optionally set the {@link Timezone} to use. If not specified {@literal UTC} is used.<br />
		 * <strong>NOTE:</strong> Requires MongoDB 3.6 or later.
		 *
		 * @param timezone must not be {@literal null}. Consider {@link Timezone#none()} instead.
		 * @return new instance of {@link Month}.
		 * @throws IllegalArgumentException if given {@literal timezone} is {@literal null}.
		 * @since 2.1
		 */
		@Override
		public Month withTimezone(Timezone timezone) {

			Assert.notNull(timezone, "Timezone must not be null");
			return new Month(appendTimezone(values().iterator().next(), timezone));
		}

		@Override
		protected String getMongoMethod() {
			return "$month";
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $week}.
	 *
	 * @author Christoph Strobl
	 * @author Matt Morrissette
	 */
	public static class Week extends TimezonedDateAggregationExpression {

		private Week(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link Week}.
		 *
		 * @param value must not be {@literal null} and resolve to field, expression or object that represents a date.
		 * @return new instance of {@link Week}.
		 * @throws IllegalArgumentException if given value is {@literal null}.
		 * @since 2.1
		 */
		public static Week week(Object value) {

			Assert.notNull(value, "value must not be null");
			return new Week(value);
		}

		/**
		 * Creates new {@link Week}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Week}.
		 */
		public static Week weekOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return week(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Week}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Week}.
		 */
		public static Week weekOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return week(expression);
		}

		/**
		 * Optionally set the {@link Timezone} to use. If not specified {@literal UTC} is used.<br />
		 * <strong>NOTE:</strong> Requires MongoDB 3.6 or later.
		 *
		 * @param timezone must not be {@literal null}. Consider {@link Timezone#none()} instead.
		 * @return new instance of {@link Week}.
		 * @throws IllegalArgumentException if given {@literal timezone} is {@literal null}.
		 * @since 2.1
		 */
		@Override
		public Week withTimezone(Timezone timezone) {

			Assert.notNull(timezone, "Timezone must not be null");
			return new Week(appendTimezone(values().iterator().next(), timezone));
		}

		@Override
		protected String getMongoMethod() {
			return "$week";
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $hour}.
	 *
	 * @author Christoph Strobl
	 * @author Matt Morrissette
	 */
	public static class Hour extends TimezonedDateAggregationExpression {

		private Hour(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link Hour}.
		 *
		 * @param value must not be {@literal null} and resolve to field, expression or object that represents a date.
		 * @return new instance of {@link Hour}.
		 * @throws IllegalArgumentException if given value is {@literal null}.
		 * @since 2.1
		 */
		public static Hour hour(Object value) {

			Assert.notNull(value, "value must not be null");
			return new Hour(value);
		}

		/**
		 * Creates new {@link Hour}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Hour}.
		 */
		public static Hour hourOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return hour(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Hour}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Hour}.
		 */
		public static Hour hourOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return hour(expression);
		}

		/**
		 * Optionally set the {@link Timezone} to use. If not specified {@literal UTC} is used.<br />
		 * <strong>NOTE:</strong> Requires MongoDB 3.6 or later.
		 *
		 * @param timezone must not be {@literal null}. Consider {@link Timezone#none()} instead.
		 * @return new instance of {@link Hour}.
		 * @throws IllegalArgumentException if given {@literal timezone} is {@literal null}.
		 * @since 2.1
		 */
		@Override
		public Hour withTimezone(Timezone timezone) {

			Assert.notNull(timezone, "Timezone must not be null");
			return new Hour(appendTimezone(values().iterator().next(), timezone));
		}

		@Override
		protected String getMongoMethod() {
			return "$hour";
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $minute}.
	 *
	 * @author Christoph Strobl
	 * @author Matt Morrissette
	 */
	public static class Minute extends TimezonedDateAggregationExpression {

		private Minute(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link Minute}.
		 *
		 * @param value must not be {@literal null} and resolve to field, expression or object that represents a date.
		 * @return new instance of {@link Minute}.
		 * @throws IllegalArgumentException if given value is {@literal null}.
		 * @since 2.1
		 */
		public static Minute minute(Object value) {

			Assert.notNull(value, "value must not be null");
			return new Minute(value);
		}

		/**
		 * Creates new {@link Minute}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Minute}.
		 */
		public static Minute minuteOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return minute(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Minute}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Minute}.
		 */
		public static Minute minuteOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return minute(expression);
		}

		/**
		 * Optionally set the {@link Timezone} to use. If not specified {@literal UTC} is used.<br />
		 * <strong>NOTE:</strong> Requires MongoDB 3.6 or later.
		 *
		 * @param timezone must not be {@literal null}. Consider {@link Timezone#none()} instead.
		 * @return new instance of {@link Minute}.
		 * @throws IllegalArgumentException if given {@literal timezone} is {@literal null}.
		 * @since 2.1
		 */
		@Override
		public Minute withTimezone(Timezone timezone) {

			Assert.notNull(timezone, "Timezone must not be null");
			return new Minute(appendTimezone(values().iterator().next(), timezone));
		}

		@Override
		protected String getMongoMethod() {
			return "$minute";
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $second}.
	 *
	 * @author Christoph Strobl
	 * @author Matt Morrissette
	 */
	public static class Second extends TimezonedDateAggregationExpression {

		private Second(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link Second}.
		 *
		 * @param value must not be {@literal null} and resolve to field, expression or object that represents a date.
		 * @return new instance of {@link Second}.
		 * @throws IllegalArgumentException if given value is {@literal null}.
		 * @since 2.1
		 */
		public static Second second(Object value) {

			Assert.notNull(value, "value must not be null");
			return new Second(value);
		}

		/**
		 * Creates new {@link Second}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Second}.
		 */
		public static Second secondOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return second(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Second}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Second}.
		 */
		public static Second secondOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return second(expression);
		}

		/**
		 * Optionally set the {@link Timezone} to use. If not specified {@literal UTC} is used.<br />
		 * <strong>NOTE:</strong> Requires MongoDB 3.6 or later.
		 *
		 * @param timezone must not be {@literal null}. Consider {@link Timezone#none()} instead.
		 * @return new instance of {@link Second}.
		 * @throws IllegalArgumentException if given {@literal timezone} is {@literal null}.
		 * @since 2.1
		 */
		@Override
		public Second withTimezone(Timezone timezone) {

			Assert.notNull(timezone, "Timezone must not be null");
			return new Second(appendTimezone(values().iterator().next(), timezone));
		}

		@Override
		protected String getMongoMethod() {
			return "$second";
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $millisecond}.
	 *
	 * @author Christoph Strobl
	 * @author Matt Morrissette
	 */
	public static class Millisecond extends TimezonedDateAggregationExpression {

		private Millisecond(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link Millisecond}.
		 *
		 * @param value must not be {@literal null} and resolve to field, expression or object that represents a date.
		 * @return new instance of {@link Millisecond}.
		 * @throws IllegalArgumentException if given value is {@literal null}.
		 * @since 2.1
		 */
		public static Millisecond millisecond(Object value) {

			Assert.notNull(value, "value must not be null");
			return new Millisecond(value);
		}

		/**
		 * Creates new {@link Millisecond}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Millisecond}.
		 */
		public static Millisecond millisecondOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return millisecond(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Millisecond}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Millisecond}.
		 */
		public static Millisecond millisecondOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return millisecond(expression);
		}

		/**
		 * Optionally set the {@link Timezone} to use. If not specified {@literal UTC} is used.<br />
		 * <strong>NOTE:</strong> Requires MongoDB 3.6 or later.
		 *
		 * @param timezone must not be {@literal null}. Consider {@link Timezone#none()} instead.
		 * @return new instance of {@link Millisecond}.
		 * @throws IllegalArgumentException if given {@literal timezone} is {@literal null}.
		 * @since 2.1
		 */
		@Override
		public Millisecond withTimezone(Timezone timezone) {

			Assert.notNull(timezone, "Timezone must not be null");
			return new Millisecond(appendTimezone(values().iterator().next(), timezone));
		}

		@Override
		protected String getMongoMethod() {
			return "$millisecond";
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $dateToString}.
	 *
	 * @author Christoph Strobl
	 * @author Matt Morrissette
	 */
	public static class DateToString extends TimezonedDateAggregationExpression {

		private DateToString(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link FormatBuilder}.
		 *
		 * @param value must not be {@literal null} and resolve to field, expression or object that represents a date.
		 * @return new instance of {@link FormatBuilder}.
		 * @throws IllegalArgumentException if given value is {@literal null}.
		 * @since 2.1
		 */
		public static FormatBuilder dateToString(Object value) {

			Assert.notNull(value, "value must not be null");

			return new FormatBuilder() {

				@Override
				public DateToString toString(String format) {

					Assert.notNull(format, "Format must not be null");
					return new DateToString(argumentMap(value, format, Timezone.none()));
				}

				@Override
				public DateToString defaultFormat() {
					return new DateToString(argumentMap(value, null, Timezone.none()));
				}
			};
		}

		/**
		 * Creates new {@link FormatBuilder} allowing to define the date format to apply.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link FormatBuilder} to crate {@link DateToString}.
		 */
		public static FormatBuilder dateOf(final String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return dateToString(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link FormatBuilder} allowing to define the date format to apply.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link FormatBuilder} to crate {@link DateToString}.
		 */
		public static FormatBuilder dateOf(final AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return dateToString(expression);
		}

		/**
		 * Optionally set the {@link Timezone} to use. If not specified {@literal UTC} is used.<br />
		 * <strong>NOTE:</strong> Requires MongoDB 3.6 or later.
		 *
		 * @param timezone must not be {@literal null}. Consider {@link Timezone#none()} instead.
		 * @return new instance of {@link Millisecond}.
		 * @throws IllegalArgumentException if given {@literal timezone} is {@literal null}.
		 * @since 2.1
		 */
		@Override
		public DateToString withTimezone(Timezone timezone) {

			Assert.notNull(timezone, "Timezone must not be null");
			return new DateToString(append("timezone", timezone));
		}

		/**
		 * Optionally specify the value to return when the date is {@literal null} or missing. <br />
		 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link DateToString}.
		 * @since 2.1
		 */
		public DateToString onNullReturn(Object value) {
			return new DateToString(append("onNull", value));
		}

		/**
		 * Optionally specify the field holding the value to return when the date is {@literal null} or missing. <br />
		 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link DateToString}.
		 * @since 2.1
		 */
		public DateToString onNullReturnValueOf(String fieldReference) {
			return onNullReturn(Fields.field(fieldReference));
		}

		/**
		 * Optionally specify the expression to evaluate and return when the date is {@literal null} or missing. <br />
		 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link DateToString}.
		 * @since 2.1
		 */
		public DateToString onNullReturnValueOf(AggregationExpression expression) {
			return onNullReturn(expression);
		}

		@Override
		protected String getMongoMethod() {
			return "$dateToString";
		}

		private static java.util.Map<String, Object> argumentMap(Object date, @Nullable String format, Timezone timezone) {

			java.util.Map<String, Object> args = new LinkedHashMap<>(2);

			if (StringUtils.hasText(format)) {
				args.put("format", format);
			}

			args.put("date", date);

			if (!ObjectUtils.nullSafeEquals(timezone, Timezone.none())) {
				args.put("timezone", timezone.value);
			}
			return args;
		}

		protected java.util.Map<String, Object> append(String key, Object value) {

			java.util.Map<String, Object> clone = new LinkedHashMap<>(argumentMap());

			if (value instanceof Timezone timezone) {

				if (ObjectUtils.nullSafeEquals(value, Timezone.none())) {
					clone.remove("timezone");
				} else {
					clone.put("timezone", timezone.value);
				}
			} else {
				clone.put(key, value);
			}

			return clone;
		}

		public interface FormatBuilder {

			/**
			 * Creates new {@link DateToString} with all previously added arguments appending the given one.
			 *
			 * @param format must not be {@literal null}.
			 * @return
			 */
			DateToString toString(String format);

			/**
			 * Creates new {@link DateToString} using the server default string format ({@code %Y-%m-%dT%H:%M:%S.%LZ}) for
			 * dates. <br />
			 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
			 *
			 * @return new instance of {@link DateToString}.
			 * @since 2.1
			 */
			DateToString defaultFormat();
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $isoDayOfWeek}.
	 *
	 * @author Christoph Strobl
	 * @author Matt Morrissette
	 */
	public static class IsoDayOfWeek extends TimezonedDateAggregationExpression {

		private IsoDayOfWeek(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link IsoDayOfWeek}.
		 *
		 * @param value must not be {@literal null} and resolve to field, expression or object that represents a date.
		 * @return new instance of {@link IsoDayOfWeek}.
		 * @throws IllegalArgumentException if given value is {@literal null}.
		 * @since 2.1
		 */
		public static IsoDayOfWeek isoDayWeek(Object value) {

			Assert.notNull(value, "value must not be null");
			return new IsoDayOfWeek(value);
		}

		/**
		 * Creates new {@link IsoDayOfWeek}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link IsoDayOfWeek}.
		 */
		public static IsoDayOfWeek isoDayOfWeek(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return isoDayWeek(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link IsoDayOfWeek}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link IsoDayOfWeek}.
		 */
		public static IsoDayOfWeek isoDayOfWeek(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return isoDayWeek(expression);
		}

		/**
		 * Optionally set the {@link Timezone} to use. If not specified {@literal UTC} is used.<br />
		 * <strong>NOTE:</strong> Requires MongoDB 3.6 or later.
		 *
		 * @param timezone must not be {@literal null}. Consider {@link Timezone#none()} instead.
		 * @return new instance of {@link IsoDayOfWeek}.
		 * @throws IllegalArgumentException if given {@literal timezone} is {@literal null}.
		 * @since 2.1
		 */
		@Override
		public IsoDayOfWeek withTimezone(Timezone timezone) {

			Assert.notNull(timezone, "Timezone must not be null");
			return new IsoDayOfWeek(appendTimezone(values().iterator().next(), timezone));
		}

		@Override
		protected String getMongoMethod() {
			return "$isoDayOfWeek";
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $isoWeek}.
	 *
	 * @author Christoph Strobl
	 * @author Matt Morrissette
	 */
	public static class IsoWeek extends TimezonedDateAggregationExpression {

		private IsoWeek(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link IsoWeek}.
		 *
		 * @param value must not be {@literal null} and resolve to field, expression or object that represents a date.
		 * @return new instance of {@link IsoWeek}.
		 * @throws IllegalArgumentException if given value is {@literal null}.
		 * @since 2.1
		 */
		public static IsoWeek isoWeek(Object value) {

			Assert.notNull(value, "value must not be null");
			return new IsoWeek(value);
		}

		/**
		 * Creates new {@link IsoWeek}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link IsoWeek}.
		 */
		public static IsoWeek isoWeekOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return isoWeek(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link IsoWeek}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link IsoWeek}.
		 */
		public static IsoWeek isoWeekOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return isoWeek(expression);
		}

		/**
		 * Optionally set the {@link Timezone} to use. If not specified {@literal UTC} is used.<br />
		 * <strong>NOTE:</strong> Requires MongoDB 3.6 or later.
		 *
		 * @param timezone must not be {@literal null}. Consider {@link Timezone#none()} instead.
		 * @return new instance of {@link IsoWeek}.
		 * @throws IllegalArgumentException if given {@literal timezone} is {@literal null}.
		 * @since 2.1
		 */
		@Override
		public IsoWeek withTimezone(Timezone timezone) {

			Assert.notNull(timezone, "Timezone must not be null");
			return new IsoWeek(appendTimezone(values().iterator().next(), timezone));
		}

		@Override
		protected String getMongoMethod() {
			return "$isoWeek";
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $isoWeekYear}.
	 *
	 * @author Christoph Strobl
	 * @author Matt Morrissette
	 */
	public static class IsoWeekYear extends TimezonedDateAggregationExpression {

		private IsoWeekYear(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link IsoWeekYear}.
		 *
		 * @param value must not be {@literal null} and resolve to field, expression or object that represents a date.
		 * @return new instance of {@link IsoWeekYear}.
		 * @throws IllegalArgumentException if given value is {@literal null}.
		 * @since 2.1
		 */
		public static IsoWeekYear isoWeekYear(Object value) {

			Assert.notNull(value, "value must not be null");
			return new IsoWeekYear(value);
		}

		/**
		 * Creates new {@link IsoWeekYear}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link IsoWeekYear}.
		 */
		public static IsoWeekYear isoWeekYearOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return isoWeekYear(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Millisecond}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link IsoWeekYear}.
		 */
		public static IsoWeekYear isoWeekYearOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return isoWeekYear(expression);
		}

		/**
		 * Optionally set the {@link Timezone} to use. If not specified {@literal UTC} is used.<br />
		 * <strong>NOTE:</strong> Requires MongoDB 3.6 or later.
		 *
		 * @param timezone must not be {@literal null}. Consider {@link Timezone#none()} instead.
		 * @return new instance of {@link IsoWeekYear}.
		 * @throws IllegalArgumentException if given {@literal timezone} is {@literal null}.
		 * @since 2.1
		 */
		@Override
		public IsoWeekYear withTimezone(Timezone timezone) {

			Assert.notNull(timezone, "Timezone must not be null");
			return new IsoWeekYear(appendTimezone(values().iterator().next(), timezone));
		}

		@Override
		protected String getMongoMethod() {
			return "$isoWeekYear";
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	public interface DateParts<T extends DateParts<T>> {

		/**
		 * Set the {@literal hour} to the given value which must resolve to a value in range of {@code 0 - 23}. Can be a
		 * simple value, {@link Field field reference} or {@link AggregationExpression expression}.
		 *
		 * @param hour must not be {@literal null}.
		 * @return new instance.
		 * @throws IllegalArgumentException if given {@literal hour} is {@literal null}
		 */
		T hour(Object hour);

		/**
		 * Set the {@literal hour} to the value resolved by following the given {@link Field field reference}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance.
		 * @throws IllegalArgumentException if given {@literal fieldReference} is {@literal null}.
		 */
		default T hourOf(String fieldReference) {
			return hour(Fields.field(fieldReference));
		}

		/**
		 * Set the {@literal hour} to the result of the given {@link AggregationExpression expression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance.
		 * @throws IllegalArgumentException if given {@literal expression} is {@literal null}.
		 */
		default T hourOf(AggregationExpression expression) {
			return hour(expression);
		}

		/**
		 * Set the {@literal minute} to the given value which must resolve to a value in range {@code 0 - 59}. Can be a
		 * simple value, {@link Field field reference} or {@link AggregationExpression expression}.
		 *
		 * @param minute must not be {@literal null}.
		 * @return new instance.
		 * @throws IllegalArgumentException if given {@literal minute} is {@literal null}
		 */
		T minute(Object minute);

		/**
		 * Set the {@literal minute} to the value resolved by following the given {@link Field field reference}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance.
		 * @throws IllegalArgumentException if given {@literal fieldReference} is {@literal null}.
		 */
		default T minuteOf(String fieldReference) {
			return minute(Fields.field(fieldReference));
		}

		/**
		 * Set the {@literal minute} to the result of the given {@link AggregationExpression expression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance.
		 * @throws IllegalArgumentException if given {@literal expression} is {@literal null}.
		 */
		default T minuteOf(AggregationExpression expression) {
			return minute(expression);
		}

		/**
		 * Set the {@literal second} to the given value which must resolve to a value in range {@code 0 - 59}. Can be a
		 * simple value, {@link Field field reference} or {@link AggregationExpression expression}.
		 *
		 * @param second must not be {@literal null}.
		 * @return new instance.
		 * @throws IllegalArgumentException if given {@literal second} is {@literal null}
		 */
		T second(Object second);

		/**
		 * Set the {@literal second} to the value resolved by following the given {@link Field field reference}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance.
		 * @throws IllegalArgumentException if given {@literal fieldReference} is {@literal null}.
		 */
		default T secondOf(String fieldReference) {
			return second(Fields.field(fieldReference));
		}

		/**
		 * Set the {@literal second} to the result of the given {@link AggregationExpression expression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance.
		 * @throws IllegalArgumentException if given {@literal expression} is {@literal null}.
		 */
		default T secondOf(AggregationExpression expression) {
			return second(expression);
		}

		/**
		 * Set the {@literal millisecond} to the given value which must resolve to a value in range {@code 0 - 999}. Can be
		 * a simple value, {@link Field field reference} or {@link AggregationExpression expression}.
		 *
		 * @param millisecond must not be {@literal null}.
		 * @return new instance.
		 * @throws IllegalArgumentException if given {@literal millisecond} is {@literal null}
		 * @since 3.2
		 */
		T millisecond(Object millisecond);

		/**
		 * Set the {@literal millisecond} to the value resolved by following the given {@link Field field reference}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance.
		 * @throws IllegalArgumentException if given {@literal fieldReference} is {@literal null}.
		 * @since 3.2
		 */
		default T millisecondOf(String fieldReference) {
			return millisecond(Fields.field(fieldReference));
		}

		/**
		 * Set the {@literal milliseconds} to the result of the given {@link AggregationExpression expression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance.
		 * @throws IllegalArgumentException if given {@literal expression} is {@literal null}.
		 * @since 3.2
		 */
		default T millisecondOf(AggregationExpression expression) {
			return millisecond(expression);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $dateFromParts}.<br />
	 * <strong>NOTE:</strong> Requires MongoDB 3.6 or later.
	 *
	 * @author Matt Morrissette
	 * @author Christoph Strobl
	 * @see <a href=
	 *      "https://docs.mongodb.com/manual/reference/operator/aggregation/dateFromParts/">https://docs.mongodb.com/manual/reference/operator/aggregation/dateFromParts/</a>
	 * @since 2.1
	 */
	public static class DateFromParts extends TimezonedDateAggregationExpression implements DateParts<DateFromParts> {

		private DateFromParts(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link DateFromPartsWithYear}.
		 *
		 * @return new instance of {@link DateFromPartsWithYear}.
		 * @since 2.1
		 */
		public static DateFromPartsWithYear dateFromParts() {
			return year -> new DateFromParts(Collections.singletonMap("year", year));
		}

		/**
		 * Set the {@literal month} to the given value which must resolve to a calendar month in range {@code 1 - 12}. Can
		 * be a simple value, {@link Field field reference} or {@link AggregationExpression expression}.
		 *
		 * @param month must not be {@literal null}.
		 * @return new instance.
		 * @throws IllegalArgumentException if given {@literal month} is {@literal null}.
		 */
		public DateFromParts month(Object month) {
			return new DateFromParts(append("month", month));
		}

		/**
		 * Set the {@literal month} to the value resolved by following the given {@link Field field reference}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance.
		 * @throws IllegalArgumentException if given {@literal fieldReference} is {@literal null}.
		 */
		public DateFromParts monthOf(String fieldReference) {
			return month(Fields.field(fieldReference));
		}

		/**
		 * Set the {@literal month} to the result of the given {@link AggregationExpression expression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance.
		 * @throws IllegalArgumentException if given {@literal expression} is {@literal null}.
		 */
		public DateFromParts monthOf(AggregationExpression expression) {
			return month(expression);
		}

		/**
		 * Set the {@literal day} to the given value which must resolve to a calendar day in range {@code 1 - 31}. Can be a
		 * simple value, {@link Field field reference} or {@link AggregationExpression expression}.
		 *
		 * @param day must not be {@literal null}.
		 * @return new instance.
		 * @throws IllegalArgumentException if given {@literal day} is {@literal null}.
		 */
		public DateFromParts day(Object day) {
			return new DateFromParts(append("day", day));
		}

		/**
		 * Set the {@literal day} to the value resolved by following the given {@link Field field reference}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance.
		 * @throws IllegalArgumentException if given {@literal fieldReference} is {@literal null}.
		 */
		public DateFromParts dayOf(String fieldReference) {
			return day(Fields.field(fieldReference));
		}

		/**
		 * Set the {@literal day} to the result of the given {@link AggregationExpression expression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance.
		 * @throws IllegalArgumentException if given {@literal expression} is {@literal null}.
		 */
		public DateFromParts dayOf(AggregationExpression expression) {
			return day(expression);
		}

		@Override
		public DateFromParts hour(Object hour) {
			return new DateFromParts(append("hour", hour));
		}

		@Override
		public DateFromParts minute(Object minute) {
			return new DateFromParts(append("minute", minute));
		}

		@Override
		public DateFromParts second(Object second) {
			return new DateFromParts(append("second", second));
		}

		@Override
		public DateFromParts millisecond(Object millisecond) {
			return new DateFromParts(append("millisecond", millisecond));
		}

		/**
		 * Optionally set the {@link Timezone} to use. If not specified {@literal UTC} is used.<br />
		 * <strong>NOTE:</strong> Requires MongoDB 3.6 or later.
		 *
		 * @param timezone must not be {@literal null}. Consider {@link Timezone#none()} instead.
		 * @return new instance of {@link DateFromParts}.
		 * @throws IllegalArgumentException if given {@literal timezone} is {@literal null}.
		 */
		@Override
		public DateFromParts withTimezone(Timezone timezone) {
			return new DateFromParts(appendTimezone(argumentMap(), timezone));
		}

		@Override
		protected String getMongoMethod() {
			return "$dateFromParts";
		}

		/**
		 * @author Christoph Strobl
		 */
		public interface DateFromPartsWithYear {

			/**
			 * Set the {@literal year} to the given value which must resolve to a calendar year. Can be a simple value,
			 * {@link Field field reference} or {@link AggregationExpression expression}.
			 *
			 * @param year must not be {@literal null}.
			 * @return new instance of {@link DateFromParts}.
			 * @throws IllegalArgumentException if given {@literal year} is {@literal null}
			 */
			DateFromParts year(Object year);

			/**
			 * Set the {@literal year} to the value resolved by following the given {@link Field field reference}.
			 *
			 * @param fieldReference must not be {@literal null}.
			 * @return new instance of {@link DateFromParts}.
			 * @throws IllegalArgumentException if given {@literal fieldReference} is {@literal null}.
			 */
			default DateFromParts yearOf(String fieldReference) {

				Assert.hasText(fieldReference, "Field reference must not be null nor empty");
				return year(Fields.field(fieldReference));
			}

			/**
			 * Set the {@literal year} to the result of the given {@link AggregationExpression expression}.
			 *
			 * @param expression must not be {@literal null}.
			 * @return new instance of {@link DateFromParts}.
			 * @throws IllegalArgumentException if given {@literal expression} is {@literal null}.
			 */
			default DateFromParts yearOf(AggregationExpression expression) {

				Assert.notNull(expression, "Expression must not be null");
				return year(expression);
			}
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $dateFromParts} using ISO week date.<br />
	 * <strong>NOTE:</strong> Requires MongoDB 3.6 or later.
	 *
	 * @author Matt Morrissette
	 * @author Christoph Strobl
	 * @see <a href=
	 *      "https://docs.mongodb.com/manual/reference/operator/aggregation/dateFromParts/">https://docs.mongodb.com/manual/reference/operator/aggregation/dateFromParts/</a>
	 * @since 2.1
	 */
	public static class IsoDateFromParts extends TimezonedDateAggregationExpression
			implements DateParts<IsoDateFromParts> {

		private IsoDateFromParts(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link IsoDateFromPartsWithYear}.
		 *
		 * @return new instance of {@link IsoDateFromPartsWithYear}.
		 * @since 2.1
		 */
		public static IsoDateFromPartsWithYear dateFromParts() {
			return year -> new IsoDateFromParts(Collections.singletonMap("isoWeekYear", year));
		}

		/**
		 * Set the {@literal week of year} to the given value which must resolve to a calendar week in range {@code 1 - 53}.
		 * Can be a simple value, {@link Field field reference} or {@link AggregationExpression expression}.
		 *
		 * @param isoWeek must not be {@literal null}.
		 * @return new instance.
		 * @throws IllegalArgumentException if given {@literal isoWeek} is {@literal null}.
		 */
		public IsoDateFromParts isoWeek(Object isoWeek) {
			return new IsoDateFromParts(append("isoWeek", isoWeek));
		}

		/**
		 * Set the {@literal week of year} to the value resolved by following the given {@link Field field reference}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance.
		 * @throws IllegalArgumentException if given {@literal fieldReference} is {@literal null}.
		 */
		public IsoDateFromParts isoWeekOf(String fieldReference) {
			return isoWeek(Fields.field(fieldReference));
		}

		/**
		 * Set the {@literal week of year} to the result of the given {@link AggregationExpression expression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance.
		 * @throws IllegalArgumentException if given {@literal expression} is {@literal null}.
		 */
		public IsoDateFromParts isoWeekOf(AggregationExpression expression) {
			return isoWeek(expression);
		}

		/**
		 * Set the {@literal day of week} to the given value which must resolve to a weekday in range {@code 1 - 7}. Can be
		 * a simple value, {@link Field field reference} or {@link AggregationExpression expression}.
		 *
		 * @param day must not be {@literal null}.
		 * @return new instance.
		 * @throws IllegalArgumentException if given {@literal isoWeek} is {@literal null}.
		 */
		public IsoDateFromParts isoDayOfWeek(Object day) {
			return new IsoDateFromParts(append("isoDayOfWeek", day));
		}

		/**
		 * Set the {@literal day of week} to the value resolved by following the given {@link Field field reference}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance.
		 * @throws IllegalArgumentException if given {@literal fieldReference} is {@literal null}.
		 */
		public IsoDateFromParts isoDayOfWeekOf(String fieldReference) {
			return isoDayOfWeek(Fields.field(fieldReference));
		}

		/**
		 * Set the {@literal day of week} to the result of the given {@link AggregationExpression expression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance.
		 * @throws IllegalArgumentException if given {@literal expression} is {@literal null}.
		 */
		public IsoDateFromParts isoDayOfWeekOf(AggregationExpression expression) {
			return isoDayOfWeek(expression);
		}

		@Override
		public IsoDateFromParts hour(Object hour) {
			return new IsoDateFromParts(append("hour", hour));
		}

		@Override
		public IsoDateFromParts minute(Object minute) {
			return new IsoDateFromParts(append("minute", minute));
		}

		@Override
		public IsoDateFromParts second(Object second) {
			return new IsoDateFromParts(append("second", second));
		}

		@Override
		public IsoDateFromParts millisecond(Object millisecond) {
			return new IsoDateFromParts(append("millisecond", millisecond));
		}

		/**
		 * Optionally set the {@link Timezone} to use. If not specified {@literal UTC} is used.<br />
		 * <strong>NOTE:</strong> Requires MongoDB 3.6 or later.
		 *
		 * @param timezone must not be {@literal null}. Consider {@link Timezone#none()} instead.
		 * @return new instance of {@link IsoDateFromParts}.
		 * @throws IllegalArgumentException if given {@literal timezone} is {@literal null}.
		 */
		@Override
		public IsoDateFromParts withTimezone(Timezone timezone) {
			return new IsoDateFromParts(appendTimezone(argumentMap(), timezone));
		}

		@Override
		protected String getMongoMethod() {
			return "$dateFromParts";
		}

		/**
		 * @author Christoph Strobl
		 */
		public interface IsoDateFromPartsWithYear {

			/**
			 * Set the {@literal week date year} to the given value which must resolve to a weekday in range {@code 0 - 9999}.
			 * Can be a simple value, {@link Field field reference} or {@link AggregationExpression expression}.
			 *
			 * @param isoWeekYear must not be {@literal null}.
			 * @return new instance.
			 * @throws IllegalArgumentException if given {@literal isoWeekYear} is {@literal null}.
			 */
			IsoDateFromParts isoWeekYear(Object isoWeekYear);

			/**
			 * Set the {@literal week date year} to the value resolved by following the given {@link Field field reference}.
			 *
			 * @param fieldReference must not be {@literal null}.
			 * @return new instance.
			 * @throws IllegalArgumentException if given {@literal fieldReference} is {@literal null}.
			 */
			default IsoDateFromParts isoWeekYearOf(String fieldReference) {

				Assert.hasText(fieldReference, "Field reference must not be null nor empty");
				return isoWeekYear(Fields.field(fieldReference));
			}

			/**
			 * Set the {@literal week date year} to the result of the given {@link AggregationExpression expression}.
			 *
			 * @param expression must not be {@literal null}.
			 * @return new instance.
			 * @throws IllegalArgumentException if given {@literal expression} is {@literal null}.
			 */
			default IsoDateFromParts isoWeekYearOf(AggregationExpression expression) {

				Assert.notNull(expression, "Expression must not be null");
				return isoWeekYear(expression);
			}
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $dateToParts}.<br />
	 * <strong>NOTE:</strong> Requires MongoDB 3.6 or later.
	 *
	 * @author Matt Morrissette
	 * @author Christoph Strobl
	 * @see <a href=
	 *      "https://docs.mongodb.com/manual/reference/operator/aggregation/dateToParts/">https://docs.mongodb.com/manual/reference/operator/aggregation/dateToParts/</a>
	 * @since 2.1
	 */
	public static class DateToParts extends TimezonedDateAggregationExpression {

		private DateToParts(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link DateToParts}.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link DateToParts}.
		 * @throws IllegalArgumentException if given {@literal value} is {@literal null}.
		 */
		public static DateToParts dateToParts(Object value) {

			Assert.notNull(value, "Value must not be null");
			return new DateToParts(Collections.singletonMap("date", value));
		}

		/**
		 * Creates new {@link DateToParts}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link DateToParts}.
		 * @throws IllegalArgumentException if given {@literal fieldReference} is {@literal null}.
		 */
		public static DateToParts datePartsOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return dateToParts(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link DateToParts}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link DateToParts}.
		 * @throws IllegalArgumentException if given {@literal expression} is {@literal null}.
		 */
		public static DateToParts datePartsOf(AggregationExpression expression) {
			return dateToParts(expression);
		}

		/**
		 * Use ISO week date fields in the resulting document.
		 *
		 * @return new instance of {@link DateToParts}.
		 */
		public DateToParts iso8601() {
			return new DateToParts(append("iso8601", true));
		}

		/**
		 * Optionally set the {@link Timezone} to use. If not specified {@literal UTC} is used.<br />
		 * <strong>NOTE:</strong> Requires MongoDB 3.6 or later.
		 *
		 * @param timezone must not be {@literal null}. Consider {@link Timezone#none()} instead.
		 * @return new instance of {@link DateFromParts}.
		 * @throws IllegalArgumentException if given {@literal timezone} is {@literal null}.
		 */
		@Override
		public DateToParts withTimezone(Timezone timezone) {
			return new DateToParts(appendTimezone(argumentMap(), timezone));
		}

		@Override
		protected String getMongoMethod() {
			return "$dateToParts";
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $dateFromString}.<br />
	 * <strong>NOTE:</strong> Requires MongoDB 3.6 or later.
	 *
	 * @author Matt Morrissette
	 * @author Christoph Strobl
	 * @see <a href=
	 *      "https://docs.mongodb.com/manual/reference/operator/aggregation/dateFromString/">https://docs.mongodb.com/manual/reference/operator/aggregation/dateFromString/</a>
	 * @since 2.1
	 */
	public static class DateFromString extends TimezonedDateAggregationExpression {

		private DateFromString(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link DateFromString}.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link DateFromString}.
		 * @throws IllegalArgumentException if given {@literal value} is {@literal null}.
		 */
		public static DateFromString fromString(Object value) {
			return new DateFromString(Collections.singletonMap("dateString", value));
		}

		/**
		 * Creates new {@link DateFromString}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link DateFromString}.
		 * @throws IllegalArgumentException if given {@literal fieldReference} is {@literal null}.
		 */
		public static DateFromString fromStringOf(String fieldReference) {
			return fromString(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link DateFromString}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link DateFromString}.
		 * @throws IllegalArgumentException if given {@literal expression} is {@literal null}.
		 */
		public static DateFromString fromStringOf(AggregationExpression expression) {
			return fromString(expression);
		}

		/**
		 * Optionally set the {@link Timezone} to use. If not specified {@literal UTC} is used.<br />
		 * <strong>NOTE:</strong> Requires MongoDB 3.6 or later.
		 *
		 * @param timezone must not be {@literal null}. Consider {@link Timezone#none()} instead.
		 * @return new instance of {@link DateFromString}.
		 * @throws IllegalArgumentException if given {@literal timezone} is {@literal null}.
		 */
		@Override
		public DateFromString withTimezone(Timezone timezone) {
			return new DateFromString(appendTimezone(argumentMap(), timezone));
		}

		/**
		 * Optionally set the date format to use. If not specified {@code %Y-%m-%dT%H:%M:%S.%LZ} is used.<br />
		 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
		 *
		 * @param format must not be {@literal null}.
		 * @return new instance of {@link DateFromString}.
		 * @throws IllegalArgumentException if given {@literal format} is {@literal null}.
		 */
		public DateFromString withFormat(String format) {

			Assert.notNull(format, "Format must not be null");
			return new DateFromString(append("format", format));
		}

		@Override
		protected String getMongoMethod() {
			return "$dateFromString";
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $dateAdd}.<br />
	 * <strong>NOTE:</strong> Requires MongoDB 5.0 or later.
	 *
	 * @author Christoph Strobl
	 * @since 3.3
	 */
	public static class DateAdd extends TimezonedDateAggregationExpression {

		private DateAdd(Object value) {
			super(value);
		}

		/**
		 * Add the number of {@literal units} of the result of the given {@link AggregationExpression expression} to a
		 * {@link #toDate(Object) start date}.
		 *
		 * @param expression must not be {@literal null}.
		 * @param unit must not be {@literal null}.
		 * @return new instance of {@link DateAdd}.
		 */
		public static DateAdd addValueOf(AggregationExpression expression, String unit) {
			return addValue(expression, unit);
		}

		/**
		 * Add the number of {@literal units} from a {@literal field} to a {@link #toDate(Object) start date}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @param unit must not be {@literal null}.
		 * @return new instance of {@link DateAdd}.
		 */
		public static DateAdd addValueOf(String fieldReference, String unit) {
			return addValue(Fields.field(fieldReference), unit);
		}

		/**
		 * Add the number of {@literal units} to a {@link #toDate(Object) start date}.
		 *
		 * @param value must not be {@literal null}.
		 * @param unit must not be {@literal null}.
		 * @return new instance of {@link DateAdd}.
		 */
		public static DateAdd addValue(Object value, String unit) {

			Map<String, Object> args = new HashMap<>();
			args.put("unit", unit);
			args.put("amount", value);
			return new DateAdd(args);
		}

		/**
		 * Define the start date, in UTC, for the addition operation.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link DateAdd}.
		 */
		public DateAdd toDateOf(AggregationExpression expression) {
			return toDate(expression);
		}

		/**
		 * Define the start date, in UTC, for the addition operation.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link DateAdd}.
		 */
		public DateAdd toDateOf(String fieldReference) {
			return toDate(Fields.field(fieldReference));
		}

		/**
		 * Define the start date, in UTC, for the addition operation.
		 *
		 * @param dateExpression anything that evaluates to a valid date. Must not be {@literal null}.
		 * @return new instance of {@link DateAdd}.
		 */
		public DateAdd toDate(Object dateExpression) {
			return new DateAdd(append("startDate", dateExpression));
		}

		/**
		 * Optionally set the {@link Timezone} to use. If not specified {@literal UTC} is used.
		 *
		 * @param timezone must not be {@literal null}. Consider {@link Timezone#none()} instead.
		 * @return new instance of {@link DateAdd}.
		 */
		public DateAdd withTimezone(Timezone timezone) {
			return new DateAdd(appendTimezone(argumentMap(), timezone));
		}

		@Override
		protected String getMongoMethod() {
			return "$dateAdd";
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $dateSubtract}.<br />
	 * <strong>NOTE:</strong> Requires MongoDB 5.0 or later.
	 *
	 * @author Christoph Strobl
	 * @since 4.0
	 */
	public static class DateSubtract extends TimezonedDateAggregationExpression {

		private DateSubtract(Object value) {
			super(value);
		}

		/**
		 * Subtract the number of {@literal units} of the result of the given {@link AggregationExpression expression} from
		 * a {@link #fromDate(Object) start date}.
		 *
		 * @param expression must not be {@literal null}.
		 * @param unit must not be {@literal null}.
		 * @return new instance of {@link DateSubtract}.
		 */
		public static DateSubtract subtractValueOf(AggregationExpression expression, String unit) {
			return subtractValue(expression, unit);
		}

		/**
		 * Subtract the number of {@literal units} from a {@literal field} from a {@link #fromDate(Object) start date}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @param unit must not be {@literal null}.
		 * @return new instance of {@link DateSubtract}.
		 */
		public static DateSubtract subtractValueOf(String fieldReference, String unit) {
			return subtractValue(Fields.field(fieldReference), unit);
		}

		/**
		 * Subtract the number of {@literal units} from a {@link #fromDate(Object) start date}.
		 *
		 * @param value must not be {@literal null}.
		 * @param unit must not be {@literal null}.
		 * @return new instance of {@link DateSubtract}.
		 */
		public static DateSubtract subtractValue(Object value, String unit) {

			Map<String, Object> args = new HashMap<>();
			args.put("unit", unit);
			args.put("amount", value);
			return new DateSubtract(args);
		}

		/**
		 * Define the start date, in UTC, for the subtraction operation.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link DateSubtract}.
		 */
		public DateSubtract fromDateOf(AggregationExpression expression) {
			return fromDate(expression);
		}

		/**
		 * Define the start date, in UTC, for the subtraction operation.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link DateSubtract}.
		 */
		public DateSubtract fromDateOf(String fieldReference) {
			return fromDate(Fields.field(fieldReference));
		}

		/**
		 * Define the start date, in UTC, for the subtraction operation.
		 *
		 * @param dateExpression anything that evaluates to a valid date. Must not be {@literal null}.
		 * @return new instance of {@link DateSubtract}.
		 */
		public DateSubtract fromDate(Object dateExpression) {
			return new DateSubtract(append("startDate", dateExpression));
		}

		/**
		 * Optionally set the {@link Timezone} to use. If not specified {@literal UTC} is used.
		 *
		 * @param timezone must not be {@literal null}. Consider {@link Timezone#none()} instead.
		 * @return new instance of {@link DateSubtract}.
		 */
		public DateSubtract withTimezone(Timezone timezone) {
			return new DateSubtract(appendTimezone(argumentMap(), timezone));
		}

		@Override
		protected String getMongoMethod() {
			return "$dateSubtract";
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $dateDiff}.<br />
	 * <strong>NOTE:</strong> Requires MongoDB 5.0 or later.
	 *
	 * @author Christoph Strobl
	 * @since 3.3
	 */
	public static class DateDiff extends TimezonedDateAggregationExpression {

		private DateDiff(Object value) {
			super(value);
		}

		/**
		 * Add the number of {@literal units} of the result of the given {@link AggregationExpression expression} to a
		 * {@link #toDate(Object) start date}.
		 *
		 * @param expression must not be {@literal null}.
		 * @param unit must not be {@literal null}.
		 * @return new instance of {@link DateAdd}.
		 */
		public static DateDiff diffValueOf(AggregationExpression expression, String unit) {
			return diffValue(expression, unit);
		}

		/**
		 * Add the number of {@literal units} from a {@literal field} to a {@link #toDate(Object) start date}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @param unit must not be {@literal null}.
		 * @return new instance of {@link DateAdd}.
		 */
		public static DateDiff diffValueOf(String fieldReference, String unit) {
			return diffValue(Fields.field(fieldReference), unit);
		}

		/**
		 * Add the number of {@literal units} to a {@link #toDate(Object) start date}.
		 *
		 * @param value must not be {@literal null}.
		 * @param unit must not be {@literal null}.
		 * @return new instance of {@link DateAdd}.
		 */
		public static DateDiff diffValue(Object value, String unit) {

			Map<String, Object> args = new HashMap<>();
			args.put("unit", unit);
			args.put("endDate", value);
			return new DateDiff(args);
		}

		/**
		 * Define the start date, in UTC, for the addition operation.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link DateAdd}.
		 */
		public DateDiff toDateOf(AggregationExpression expression) {
			return toDate(expression);
		}

		/**
		 * Define the start date, in UTC, for the addition operation.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link DateAdd}.
		 */
		public DateDiff toDateOf(String fieldReference) {
			return toDate(Fields.field(fieldReference));
		}

		/**
		 * Define the start date, in UTC, for the addition operation.
		 *
		 * @param dateExpression anything that evaluates to a valid date. Must not be {@literal null}.
		 * @return new instance of {@link DateAdd}.
		 */
		public DateDiff toDate(Object dateExpression) {
			return new DateDiff(append("startDate", dateExpression));
		}

		/**
		 * Optionally set the {@link Timezone} to use. If not specified {@literal UTC} is used.
		 *
		 * @param timezone must not be {@literal null}. Consider {@link Timezone#none()} instead.
		 * @return new instance of {@link DateAdd}.
		 */
		public DateDiff withTimezone(Timezone timezone) {
			return new DateDiff(appendTimezone(argumentMap(), timezone));
		}

		/**
		 * Set the start day of the week if the unit if measure is set to {@literal week}. Uses {@literal Sunday} by
		 * default.
		 *
		 * @param day must not be {@literal null}.
		 * @return new instance of {@link DateDiff}.
		 */
		public DateDiff startOfWeek(Object day) {
			return new DateDiff(append("startOfWeek", day));
		}

		@Override
		protected String getMongoMethod() {
			return "$dateDiff";
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $dateTrunc}.<br />
	 * <strong>NOTE:</strong> Requires MongoDB 5.0 or later.
	 *
	 * @author Christoph Strobl
	 * @since 4.0
	 */
	public static class DateTrunc extends TimezonedDateAggregationExpression {

		private DateTrunc(Object value) {
			super(value);
		}

		/**
		 * Truncates the date value of computed by the given {@link AggregationExpression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link DateTrunc}.
		 */
		public static DateTrunc truncateValueOf(AggregationExpression expression) {
			return truncateValue(expression);
		}

		/**
		 * Truncates the date value of the referenced {@literal field}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link DateTrunc}.
		 */
		public static DateTrunc truncateValueOf(String fieldReference) {
			return truncateValue(Fields.field(fieldReference));
		}

		/**
		 * Truncates the date value.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link DateTrunc}.
		 */
		public static DateTrunc truncateValue(Object value) {
			return new DateTrunc(Collections.singletonMap("date", value));
		}

		/**
		 * Define the unit of time.
		 *
		 * @param unit must not be {@literal null}.
		 * @return new instance of {@link DateTrunc}.
		 */
		public DateTrunc to(String unit) {
			return new DateTrunc(append("unit", unit));
		}

		/**
		 * Define the unit of time via an {@link AggregationExpression}.
		 *
		 * @param unit must not be {@literal null}.
		 * @return new instance of {@link DateTrunc}.
		 */
		public DateTrunc to(AggregationExpression unit) {
			return new DateTrunc(append("unit", unit));
		}

		/**
		 * Define the weeks starting day if {@link #to(String)} resolves to {@literal week}.
		 *
		 * @param day must not be {@literal null}.
		 * @return new instance of {@link DateTrunc}.
		 */
		public DateTrunc startOfWeek(java.time.DayOfWeek day) {
			return startOfWeek(day.name().toLowerCase(Locale.US));
		}

		/**
		 * Define the weeks starting day if {@link #to(String)} resolves to {@literal week}.
		 *
		 * @param day must not be {@literal null}.
		 * @return new instance of {@link DateTrunc}.
		 */
		public DateTrunc startOfWeek(String day) {
			return new DateTrunc(append("startOfWeek", day));
		}

		/**
		 * Define the numeric time value.
		 *
		 * @param binSize must not be {@literal null}.
		 * @return new instance of {@link DateTrunc}.
		 */
		public DateTrunc binSize(int binSize) {
			return binSize((Object) binSize);
		}

		/**
		 * Define the numeric time value via an {@link AggregationExpression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link DateTrunc}.
		 */
		public DateTrunc binSize(AggregationExpression expression) {
			return binSize((Object) expression);
		}

		/**
		 * Define the numeric time value.
		 *
		 * @param binSize must not be {@literal null}.
		 * @return new instance of {@link DateTrunc}.
		 */
		public DateTrunc binSize(Object binSize) {
			return new DateTrunc(append("binSize", binSize));
		}

		/**
		 * Optionally set the {@link Timezone} to use. If not specified {@literal UTC} is used.
		 *
		 * @param timezone must not be {@literal null}. Consider {@link Timezone#none()} instead.
		 * @return new instance of {@link DateTrunc}.
		 */
		public DateTrunc withTimezone(Timezone timezone) {
			return new DateTrunc(appendTimezone(argumentMap(), timezone));
		}

		@Override
		protected String getMongoMethod() {
			return "$dateTrunc";
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $tsIncrement}.
	 *
	 * @author Christoph Strobl
	 * @since 4.0
	 */
	public static class TsIncrement extends AbstractAggregationExpression {

		private TsIncrement(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link TsIncrement} that returns the incrementing ordinal from a timestamp.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link TsIncrement}.
		 * @throws IllegalArgumentException if given {@literal value} is {@literal null}.
		 */
		public static TsIncrement tsIncrement(Object value) {

			Assert.notNull(value, "Value must not be null");
			return new TsIncrement(value);
		}

		/**
		 * Creates new {@link TsIncrement} that returns the incrementing ordinal from a timestamp.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link TsIncrement}.
		 * @throws IllegalArgumentException if given {@literal fieldReference} is {@literal null}.
		 */
		public static TsIncrement tsIncrementValueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return tsIncrement(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link TsIncrement}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link TsIncrement}.
		 * @throws IllegalArgumentException if given {@literal expression} is {@literal null}.
		 */
		public static TsIncrement tsIncrementValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return tsIncrement(expression);
		}

		@Override
		protected String getMongoMethod() {
			return "$tsIncrement";
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $tsSecond}.
	 *
	 * @author Christoph Strobl
	 * @since 4.0
	 */
	public static class TsSecond extends AbstractAggregationExpression {

		private TsSecond(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link TsSecond} that returns the incrementing ordinal from a timestamp.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link TsSecond}.
		 * @throws IllegalArgumentException if given {@literal value} is {@literal null}.
		 */
		public static TsSecond tsSecond(Object value) {

			Assert.notNull(value, "Value must not be null");
			return new TsSecond(value);
		}

		/**
		 * Creates new {@link TsSecond} that returns the incrementing ordinal from a timestamp.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link TsSecond}.
		 * @throws IllegalArgumentException if given {@literal fieldReference} is {@literal null}.
		 */
		public static TsSecond tsSecondValueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return tsSecond(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link TsSecond}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link TsSecond}.
		 * @throws IllegalArgumentException if given {@literal expression} is {@literal null}.
		 */
		public static TsSecond tsSecondValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return tsSecond(expression);
		}

		@Override
		protected String getMongoMethod() {
			return "$tsSecond";
		}
	}

	/**
	 * Interface defining a temporal unit for date operators.
	 *
	 * @author Mark Paluch
	 * @since 3.3
	 */
	public interface TemporalUnit {

		String name();

		/**
		 * Converts the given time unit into a {@link TemporalUnit}. Supported units are: days, hours, minutes, seconds, and
		 * milliseconds.
		 *
		 * @param timeUnit the time unit to convert, must not be {@literal null}.
		 * @return
		 * @throws IllegalArgumentException if the {@link TimeUnit} is {@literal null} or not supported for conversion.
		 */
		static TemporalUnit from(TimeUnit timeUnit) {

			Assert.notNull(timeUnit, "TimeUnit must not be null");

			switch (timeUnit) {
				case DAYS:
					return TemporalUnits.DAY;
				case HOURS:
					return TemporalUnits.HOUR;
				case MINUTES:
					return TemporalUnits.MINUTE;
				case SECONDS:
					return TemporalUnits.SECOND;
				case MILLISECONDS:
					return TemporalUnits.MILLISECOND;
			}

			throw new IllegalArgumentException(String.format("Cannot create TemporalUnit from %s", timeUnit));
		}

		/**
		 * Converts the given chrono unit into a {@link TemporalUnit}. Supported units are: years, weeks, months, days,
		 * hours, minutes, seconds, and millis.
		 *
		 * @param chronoUnit the chrono unit to convert, must not be {@literal null}.
		 * @return
		 * @throws IllegalArgumentException if the {@link TimeUnit} is {@literal null} or not supported for conversion.
		 */
		static TemporalUnit from(ChronoUnit chronoUnit) {

			switch (chronoUnit) {
				case YEARS:
					return TemporalUnits.YEAR;
				case WEEKS:
					return TemporalUnits.WEEK;
				case MONTHS:
					return TemporalUnits.MONTH;
				case DAYS:
					return TemporalUnits.DAY;
				case HOURS:
					return TemporalUnits.HOUR;
				case MINUTES:
					return TemporalUnits.MINUTE;
				case SECONDS:
					return TemporalUnits.SECOND;
				case MILLIS:
					return TemporalUnits.MILLISECOND;
			}

			throw new IllegalArgumentException(String.format("Cannot create TemporalUnit from %s", chronoUnit));
		}
	}

	/**
	 * Supported temporal units.
	 */
	enum TemporalUnits implements TemporalUnit {
		YEAR, QUARTER, WEEK, MONTH, DAY, HOUR, MINUTE, SECOND, MILLISECOND

	}

	@SuppressWarnings("unchecked")
	private static <T extends TimezonedDateAggregationExpression> T applyTimezone(T instance, Timezone timezone) {
		return !ObjectUtils.nullSafeEquals(Timezone.none(), timezone) && !instance.hasTimezone()
				? (T) instance.withTimezone(timezone)
				: instance;
	}
}
