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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

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
	 * Take the given value as date.
	 * <p/>
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

		Assert.notNull(value, "Value must not be null!");
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
	 * <table valign="top">
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
	 * <strong>NOTE: </strong>Support for timezones in aggregations Requires MongoDB 3.6 or later.
	 *
	 * @author Christoph Strobl
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

			Assert.notNull(value, "Value must not be null!");
			return new Timezone(value);
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
		 * @param value the {@link AggregationExpression} resulting in the timezone.
		 * @return new instance of {@link Timezone}.
		 */
		public static Timezone ofExpression(AggregationExpression expression) {
			return valueOf(expression);
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

			Assert.notNull(fieldReference, "FieldReference must not be null!");
		}

		/**
		 * Creates new {@link DateOperatorFactory} for given {@link AggregationExpression}.
		 *
		 * @param expression must not be {@literal null}.
		 */
		public DateOperatorFactory(AggregationExpression expression) {

			this(null, expression, null, Timezone.none());

			Assert.notNull(expression, "Expression must not be null!");
		}

		/**
		 * Creates new {@link DateOperatorFactory} for given {@code value} that resolves to a Date.
		 * <p/>
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

			Assert.notNull(value, "Value must not be null!");
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

			Assert.notNull(timezone, "Timezone must not be null!");
			return new DateOperatorFactory(fieldReference, expression, dateValue, timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the day of the year for a date as a number between 1 and
		 * 366.
		 *
		 * @return
		 */
		public DayOfYear dayOfYear() {
			return applyTimezone(DayOfYear.dayOfYear(dateReference()), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the day of the month for a date as a number between 1 and
		 * 31.
		 *
		 * @return
		 */
		public DayOfMonth dayOfMonth() {
			return applyTimezone(DayOfMonth.dayOfMonth(dateReference()), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the day of the week for a date as a number between 1
		 * (Sunday) and 7 (Saturday).
		 *
		 * @return
		 */
		public DayOfWeek dayOfWeek() {
			return applyTimezone(DayOfWeek.dayOfWeek(dateReference()), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the year portion of a date.
		 *
		 * @return
		 */
		public Year year() {
			return applyTimezone(Year.year(dateReference()), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the month of a date as a number between 1 and 12.
		 *
		 * @return
		 */
		public Month month() {
			return applyTimezone(Month.month(dateReference()), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the week of the year for a date as a number between 0 and
		 * 53.
		 *
		 * @return
		 */
		public Week week() {
			return applyTimezone(Week.week(dateReference()), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the hour portion of a date as a number between 0 and 23.
		 *
		 * @return
		 */
		public Hour hour() {
			return applyTimezone(Hour.hour(dateReference()), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the minute portion of a date as a number between 0 and 59.
		 *
		 * @return
		 */
		public Minute minute() {
			return applyTimezone(Minute.minute(dateReference()), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the second portion of a date as a number between 0 and 59,
		 * but can be 60 to account for leap seconds.
		 *
		 * @return
		 */
		public Second second() {
			return applyTimezone(Second.second(dateReference()), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the millisecond portion of a date as an integer between 0
		 * and 999.
		 *
		 * @return
		 */
		public Millisecond millisecond() {
			return applyTimezone(Millisecond.millisecond(dateReference()), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that converts a date object to a string according to a user-specified
		 * {@literal format}.
		 *
		 * @param format must not be {@literal null}.
		 * @return
		 */
		public DateToString toString(String format) {
			return applyTimezone(DateToString.dateToString(dateReference()).toString(format), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the weekday number in ISO 8601-2018 format, ranging from 1
		 * (for Monday) to 7 (for Sunday).
		 *
		 * @return
		 */
		public IsoDayOfWeek isoDayOfWeek() {
			return applyTimezone(IsoDayOfWeek.isoDayWeek(dateReference()), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the week number in ISO 8601-2018 format, ranging from 1 to
		 * 53.
		 *
		 * @return
		 */
		public IsoWeek isoWeek() {
			return applyTimezone(IsoWeek.isoWeek(dateReference()), timezone);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns the year number in ISO 8601-2018 format.
		 *
		 * @return
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

			Assert.notNull(timezone, "Timezone must not be null!");
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

			if (source instanceof Map) {
				args = new LinkedHashMap<>((Map) source);
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

			Assert.notNull(value, "value must not be null!");
			return new DayOfYear(value);
		}

		/**
		 * Creates new {@link DayOfYear}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static DayOfYear dayOfYear(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return dayOfYear(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link DayOfYear}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static DayOfYear dayOfYear(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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

			Assert.notNull(timezone, "Timezone must not be null.");
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

			Assert.notNull(value, "value must not be null!");
			return new DayOfMonth(value);
		}

		/**
		 * Creates new {@link DayOfMonth}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static DayOfMonth dayOfMonth(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return dayOfMonth(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link DayOfMonth}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static DayOfMonth dayOfMonth(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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

			Assert.notNull(timezone, "Timezone must not be null.");
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

			Assert.notNull(value, "value must not be null!");
			return new DayOfWeek(value);
		}

		/**
		 * Creates new {@link DayOfWeek}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static DayOfWeek dayOfWeek(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return dayOfWeek(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link DayOfWeek}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static DayOfWeek dayOfWeek(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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

			Assert.notNull(timezone, "Timezone must not be null.");
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

			Assert.notNull(value, "value must not be null!");
			return new Year(value);
		}

		/**
		 * Creates new {@link Year}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Year yearOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return year(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Year}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Year yearOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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

			Assert.notNull(timezone, "Timezone must not be null.");
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

			Assert.notNull(value, "value must not be null!");
			return new Month(value);
		}

		/**
		 * Creates new {@link Month}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Month monthOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return month(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Month}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Month monthOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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

			Assert.notNull(timezone, "Timezone must not be null.");
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

			Assert.notNull(value, "value must not be null!");
			return new Week(value);
		}

		/**
		 * Creates new {@link Week}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Week weekOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return week(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Week}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Week weekOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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

			Assert.notNull(timezone, "Timezone must not be null.");
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

			Assert.notNull(value, "value must not be null!");
			return new Hour(value);
		}

		/**
		 * Creates new {@link Hour}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Hour hourOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return hour(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Hour}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Hour hourOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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

			Assert.notNull(timezone, "Timezone must not be null.");
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

			Assert.notNull(value, "value must not be null!");
			return new Minute(value);
		}

		/**
		 * Creates new {@link Minute}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Minute minuteOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return minute(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Minute}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Minute minuteOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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

			Assert.notNull(timezone, "Timezone must not be null.");
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

			Assert.notNull(value, "value must not be null!");
			return new Second(value);
		}

		/**
		 * Creates new {@link Second}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Second secondOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return second(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Second}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Second secondOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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

			Assert.notNull(timezone, "Timezone must not be null.");
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

			Assert.notNull(value, "value must not be null!");
			return new Millisecond(value);
		}

		/**
		 * Creates new {@link Millisecond}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static Millisecond millisecondOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return millisecond(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Millisecond}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static Millisecond millisecondOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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

			Assert.notNull(timezone, "Timezone must not be null.");
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

			Assert.notNull(value, "value must not be null!");

			return new FormatBuilder() {

				@Override
				public DateToString toString(String format) {

					Assert.notNull(format, "Format must not be null!");
					return new DateToString(argumentMap(value, format, Timezone.none()));
				}
			};
		}

		/**
		 * Creates new {@link FormatBuilder} allowing to define the date format to apply.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static FormatBuilder dateOf(final String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return dateToString(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link FormatBuilder} allowing to define the date format to apply.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static FormatBuilder dateOf(final AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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

			Assert.notNull(timezone, "Timezone must not be null.");
			return new DateToString(argumentMap(get("date"), get("format"), timezone));
		}

		@Override
		protected String getMongoMethod() {
			return "$dateToString";
		}

		private static java.util.Map<String, Object> argumentMap(Object date, String format, Timezone timezone) {

			java.util.Map<String, Object> args = new LinkedHashMap<String, Object>(2);
			args.put("format", format);
			args.put("date", date);

			if (!ObjectUtils.nullSafeEquals(timezone, Timezone.none())) {
				args.put("timezone", timezone.value);
			}
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

			Assert.notNull(value, "value must not be null!");
			return new IsoDayOfWeek(value);
		}

		/**
		 * Creates new {@link IsoDayOfWeek}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static IsoDayOfWeek isoDayOfWeek(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return isoDayWeek(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link IsoDayOfWeek}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static IsoDayOfWeek isoDayOfWeek(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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

			Assert.notNull(timezone, "Timezone must not be null.");
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

			Assert.notNull(value, "value must not be null!");
			return new IsoWeek(value);
		}

		/**
		 * Creates new {@link IsoWeek}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static IsoWeek isoWeekOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return isoWeek(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link IsoWeek}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static IsoWeek isoWeekOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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

			Assert.notNull(timezone, "Timezone must not be null.");
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

			Assert.notNull(value, "value must not be null!");
			return new IsoWeekYear(value);
		}

		/**
		 * Creates new {@link IsoWeekYear}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return
		 */
		public static IsoWeekYear isoWeekYearOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return isoWeekYear(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Millisecond}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static IsoWeekYear isoWeekYearOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
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

			Assert.notNull(timezone, "Timezone must not be null.");
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
		 * Set the {@literal milliseconds} to the given value which must resolve to a value in range {@code 0 - 999}. Can be
		 * a simple value, {@link Field field reference} or {@link AggregationExpression expression}.
		 *
		 * @param milliseconds must not be {@literal null}.
		 * @return new instance.
		 * @throws IllegalArgumentException if given {@literal milliseconds} is {@literal null}
		 */
		T milliseconds(Object milliseconds);

		/**
		 * Set the {@literal milliseconds} to the value resolved by following the given {@link Field field reference}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance.
		 * @throws IllegalArgumentException if given {@literal fieldReference} is {@literal null}.
		 */
		default T millisecondsOf(String fieldReference) {
			return milliseconds(Fields.field(fieldReference));
		}

		/**
		 * Set the {@literal milliseconds} to the result of the given {@link AggregationExpression expression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance.
		 * @throws IllegalArgumentException if given {@literal expression} is {@literal null}.
		 */
		default T millisecondsOf(AggregationExpression expression) {
			return milliseconds(expression);
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
		public DateFromParts milliseconds(Object milliseconds) {
			return new DateFromParts(append("milliseconds", milliseconds));
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

				Assert.hasText(fieldReference, "Field reference must not be null nor empty.");
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

				Assert.notNull(expression, "Expression must not be null!");
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
		public IsoDateFromParts milliseconds(Object milliseconds) {
			return new IsoDateFromParts(append("milliseconds", milliseconds));
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

				Assert.hasText(fieldReference, "Field reference must not be null nor empty.");
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

				Assert.notNull(expression, "Expression must not be null!");
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

			Assert.notNull(value, "Value must not be null!");
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

			Assert.notNull(fieldReference, "FieldReference must not be null!");
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

		@Override
		protected String getMongoMethod() {
			return "$dateFromString";
		}
	}

	@SuppressWarnings("unchecked")
	private static <T extends TimezonedDateAggregationExpression> T applyTimezone(T instance, Timezone timezone) {
		return !ObjectUtils.nullSafeEquals(Timezone.none(), timezone) && !instance.hasTimezone()
				? (T) instance.withTimezone(timezone) : instance;
	}
}
