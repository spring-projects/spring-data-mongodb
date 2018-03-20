/*
 * Copyright 2016-2018 the original author or authors.
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

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.springframework.data.mongodb.core.aggregation.DateOperators.*;
import static org.springframework.data.mongodb.core.aggregation.LiteralOperators.Literal.*;

import java.util.Date;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Unit tests for {@link DateOperators}. DATAMONGO-1834 - Add support for aggregation operators $dateFromString,
 * $dateFromParts and $dateToParts This test case now covers all existing methods in the DateOperators class as well as
 * those added as part of DATAMONGO-1834
 *
 * @author Matt Morrissette
 */
public class DateOperatorsUnitTests {

	private static final String FIELD = "field";

	private static final String VAR = "$";

	private static final String VAR_FIELD = VAR + FIELD;

	private static final String TIMEZONE = "America/Los_Angeles";

	private static final String TIMEZONE2 = "America/New_York";

	private static final String FORMAT = "%Y-%m-%d";

	private static final DBObject LITERAL = new BasicDBObject("$literal", VAR_FIELD);

	private static final String TO_STRING_OP = "$dateToString";

	private static Object CURRENT_DATE;

	@Test(expected = IllegalArgumentException.class)
	public void rejectsEmptyFieldName() {
		dateOf("");
	}

	@Test
	public void shouldRenderFieldCorrectly() {

		final DateOperatorFactory f = dateOf(FIELD);
		assertDateFieldOp(f.dayOfMonth(), "dayOfMonth");
		assertDateFieldOp(f.dayOfWeek(), "dayOfWeek");
		assertDateFieldOp(f.dayOfYear(), "dayOfYear");
		assertDateFieldOp(f.hour(), "hour");
		assertDateFieldOp(f.isoDayOfWeek(), "isoDayOfWeek");
		assertDateFieldOp(f.isoWeek(), "isoWeek");
		assertDateFieldOp(f.isoWeekYear(), "isoWeekYear");
		assertDateFieldOp(f.millisecond(), "millisecond");
		assertDateFieldOp(f.minute(), "minute");
		assertDateFieldOp(f.month(), "month");
		assertDateFieldOp(f.second(), "second");
		assertDateFieldOp(f.week(), "week");
		assertDateFieldOp(f.year(), "year");
		assertQuarterFieldOp(f.quarter());
		assertDateFromStringField(f.fromString());
		assertDateToPartsField(f.toParts(), null);
		assertDateToPartsField(f.toIsoWeekParts(), true);
		assertDateToPartsField(f.toParts(true), true);
		assertDateToPartsField(f.toParts(false), false);
		assertDateFieldStringNoTimezoneOp(f.toString(FORMAT));
		assertDateFieldTimezoneOp(f.dayOfMonth(TIMEZONE), "dayOfMonth");
		assertDateFieldTimezoneOp(f.dayOfWeek(TIMEZONE), "dayOfWeek");
		assertDateFieldTimezoneOp(f.dayOfYear(TIMEZONE), "dayOfYear");
		assertDateFieldTimezoneOp(f.hour(TIMEZONE), "hour");
		assertDateFieldTimezoneOp(f.isoDayOfWeek(TIMEZONE), "isoDayOfWeek");
		assertDateFieldTimezoneOp(f.isoWeek(TIMEZONE), "isoWeek");
		assertDateFieldTimezoneOp(f.isoWeekYear(TIMEZONE), "isoWeekYear");
		assertDateFieldTimezoneOp(f.millisecond(TIMEZONE), "millisecond");
		assertDateFieldTimezoneOp(f.minute(TIMEZONE), "minute");
		assertDateFieldTimezoneOp(f.month(TIMEZONE), "month");
		assertDateFieldTimezoneOp(f.second(TIMEZONE), "second");
		assertDateFieldTimezoneOp(f.week(TIMEZONE), "week");
		assertDateFieldTimezoneOp(f.year(TIMEZONE), "year");
		assertDateToPartsFieldTimezone(f.toParts(TIMEZONE), null);
		assertDateToPartsFieldTimezone(f.toIsoWeekParts(TIMEZONE), true);
		assertDateToPartsFieldTimezone(f.toParts(TIMEZONE, true), true);
		assertDateToPartsFieldTimezone(f.toParts(TIMEZONE, false), false);
		assertQuarterFieldTimezoneOp(f.quarter(TIMEZONE));
		assertDateFromStringFieldTimezone(f.fromString(TIMEZONE));
		assertDateFieldStringTimezoneOp(f.toString(FORMAT, TIMEZONE));
	}

	@Test
	public void shouldRenderFieldTimezoneCorrectly() {

		final DateOperatorFactory f = dateOfWithTimezone(FIELD, TIMEZONE);
		assertDateFieldTimezoneOp(f.dayOfMonth(), "dayOfMonth");
		assertDateFieldTimezoneOp(f.dayOfWeek(), "dayOfWeek");
		assertDateFieldTimezoneOp(f.dayOfYear(), "dayOfYear");
		assertDateFieldTimezoneOp(f.hour(), "hour");
		assertDateFieldTimezoneOp(f.isoDayOfWeek(), "isoDayOfWeek");
		assertDateFieldTimezoneOp(f.isoWeek(), "isoWeek");
		assertDateFieldTimezoneOp(f.isoWeekYear(), "isoWeekYear");
		assertDateFieldTimezoneOp(f.millisecond(), "millisecond");
		assertDateFieldTimezoneOp(f.minute(), "minute");
		assertDateFieldTimezoneOp(f.month(), "month");
		assertDateFieldTimezoneOp(f.second(), "second");
		assertDateFieldTimezoneOp(f.week(), "week");
		assertDateFieldTimezoneOp(f.year(), "year");
		assertQuarterFieldTimezoneOp(f.quarter());
		assertDateFromStringFieldTimezone(f.fromString());
		assertDateToPartsFieldTimezone(f.toParts(), null);
		assertDateToPartsFieldTimezone(f.toIsoWeekParts(), true);
		assertDateToPartsFieldTimezone(f.toParts(true), true);
		assertDateToPartsFieldTimezone(f.toParts(false), false);
		assertDateFieldStringTimezoneOp(f.toString(FORMAT));

		assertDateFieldTimezone2Op(f.dayOfMonth(TIMEZONE2), "dayOfMonth");
		assertDateFieldTimezone2Op(f.dayOfWeek(TIMEZONE2), "dayOfWeek");
		assertDateFieldTimezone2Op(f.dayOfYear(TIMEZONE2), "dayOfYear");
		assertDateFieldTimezone2Op(f.hour(TIMEZONE2), "hour");
		assertDateFieldTimezone2Op(f.isoDayOfWeek(TIMEZONE2), "isoDayOfWeek");
		assertDateFieldTimezone2Op(f.isoWeek(TIMEZONE2), "isoWeek");
		assertDateFieldTimezone2Op(f.isoWeekYear(TIMEZONE2), "isoWeekYear");
		assertDateFieldTimezone2Op(f.millisecond(TIMEZONE2), "millisecond");
		assertDateFieldTimezone2Op(f.minute(TIMEZONE2), "minute");
		assertDateFieldTimezone2Op(f.month(TIMEZONE2), "month");
		assertDateFieldTimezone2Op(f.second(TIMEZONE2), "second");
		assertDateFieldTimezone2Op(f.week(TIMEZONE2), "week");
		assertDateFieldTimezone2Op(f.year(TIMEZONE2), "year");
		assertQuarterFieldTimezone2Op(f.quarter(TIMEZONE2));
		assertDateFromStringFieldTimezone2(f.fromString(TIMEZONE2));
		assertDateToPartsFieldTimezone2(f.toParts(TIMEZONE2), null);
		assertDateToPartsFieldTimezone2(f.toIsoWeekParts(TIMEZONE2), true);
		assertDateToPartsFieldTimezone2(f.toParts(TIMEZONE2, true), true);
		assertDateToPartsFieldTimezone2(f.toParts(TIMEZONE2, false), false);
		assertDateFieldStringTimezone2Op(f.toString(FORMAT, TIMEZONE2));

		assertDateFieldOp(f.dayOfMonth(null), "dayOfMonth");
		assertDateFieldOp(f.dayOfWeek(null), "dayOfWeek");
		assertDateFieldOp(f.dayOfYear(null), "dayOfYear");
		assertDateFieldOp(f.hour(null), "hour");
		assertDateFieldOp(f.isoDayOfWeek(null), "isoDayOfWeek");
		assertDateFieldOp(f.isoWeek(null), "isoWeek");
		assertDateFieldOp(f.isoWeekYear(null), "isoWeekYear");
		assertDateFieldOp(f.millisecond(null), "millisecond");
		assertDateFieldOp(f.minute(null), "minute");
		assertDateFieldOp(f.month(null), "month");
		assertDateFieldOp(f.second(null), "second");
		assertDateFieldOp(f.week(null), "week");
		assertDateFieldOp(f.year(null), "year");
		assertQuarterFieldOp(f.quarter(null));
		assertDateFromStringField(f.fromString(null));
		assertDateToPartsField(f.toParts((String) null), null);
		assertDateToPartsField(f.toIsoWeekParts(null), true);
		assertDateToPartsField(f.toParts(null, true), true);
		assertDateToPartsField(f.toParts(null, false), false);
		assertDateFieldStringNoTimezoneOp(f.toString(FORMAT, null));
	}

	@Test
	public void shouldRenderExprCorrectly() {

		final DateOperatorFactory f = dateOf(asLiteral(VAR_FIELD));
		assertDateExprOp(f.dayOfMonth(), "dayOfMonth");
		assertDateExprOp(f.dayOfWeek(), "dayOfWeek");
		assertDateExprOp(f.dayOfYear(), "dayOfYear");
		assertDateExprOp(f.hour(), "hour");
		assertDateExprOp(f.isoDayOfWeek(), "isoDayOfWeek");
		assertDateExprOp(f.isoWeek(), "isoWeek");
		assertDateExprOp(f.isoWeekYear(), "isoWeekYear");
		assertDateExprOp(f.millisecond(), "millisecond");
		assertDateExprOp(f.minute(), "minute");
		assertDateExprOp(f.month(), "month");
		assertDateExprOp(f.second(), "second");
		assertDateExprOp(f.week(), "week");
		assertDateExprOp(f.year(), "year");
		assertQuarterExprOp(f.quarter());
		assertDateFromStringExpr(f.fromString());
		assertDateToPartsExpr(f.toParts(), null);
		assertDateToPartsExpr(f.toIsoWeekParts(), true);
		assertDateToPartsExpr(f.toParts(true), true);
		assertDateToPartsExpr(f.toParts(false), false);
		assertDateExprStringNoTimezoneOp(f.toString(FORMAT));
		assertDateExprTimezoneOp(f.dayOfMonth(TIMEZONE), "dayOfMonth");
		assertDateExprTimezoneOp(f.dayOfWeek(TIMEZONE), "dayOfWeek");
		assertDateExprTimezoneOp(f.dayOfYear(TIMEZONE), "dayOfYear");
		assertDateExprTimezoneOp(f.hour(TIMEZONE), "hour");
		assertDateExprTimezoneOp(f.isoDayOfWeek(TIMEZONE), "isoDayOfWeek");
		assertDateExprTimezoneOp(f.isoWeek(TIMEZONE), "isoWeek");
		assertDateExprTimezoneOp(f.isoWeekYear(TIMEZONE), "isoWeekYear");
		assertDateExprTimezoneOp(f.millisecond(TIMEZONE), "millisecond");
		assertDateExprTimezoneOp(f.minute(TIMEZONE), "minute");
		assertDateExprTimezoneOp(f.month(TIMEZONE), "month");
		assertDateExprTimezoneOp(f.second(TIMEZONE), "second");
		assertDateExprTimezoneOp(f.week(TIMEZONE), "week");
		assertDateExprTimezoneOp(f.year(TIMEZONE), "year");
		assertQuarterExprTimezoneOp(f.quarter(TIMEZONE));
		assertDateFromStringExprTimezone(f.fromString(TIMEZONE));
		assertDateToPartsExprTimezone(f.toParts(TIMEZONE), null);
		assertDateToPartsExprTimezone(f.toIsoWeekParts(TIMEZONE), true);
		assertDateToPartsExprTimezone(f.toParts(TIMEZONE, true), true);
		assertDateToPartsExprTimezone(f.toParts(TIMEZONE, false), false);
		assertDateExprStringTimezoneOp(f.toString(FORMAT, TIMEZONE));
	}

	@Test
	public void shouldRenderExprTimezoneCorrectly() {

		final DateOperatorFactory f = dateOfWithTimezone(asLiteral(VAR_FIELD), TIMEZONE);
		assertDateExprTimezoneOp(f.dayOfMonth(), "dayOfMonth");
		assertDateExprTimezoneOp(f.dayOfWeek(), "dayOfWeek");
		assertDateExprTimezoneOp(f.dayOfYear(), "dayOfYear");
		assertDateExprTimezoneOp(f.hour(), "hour");
		assertDateExprTimezoneOp(f.isoDayOfWeek(), "isoDayOfWeek");
		assertDateExprTimezoneOp(f.isoWeek(), "isoWeek");
		assertDateExprTimezoneOp(f.isoWeekYear(), "isoWeekYear");
		assertDateExprTimezoneOp(f.millisecond(), "millisecond");
		assertDateExprTimezoneOp(f.minute(), "minute");
		assertDateExprTimezoneOp(f.month(), "month");
		assertDateExprTimezoneOp(f.second(), "second");
		assertDateExprTimezoneOp(f.week(), "week");
		assertDateExprTimezoneOp(f.year(), "year");
		assertQuarterExprTimezoneOp(f.quarter());
		assertDateFromStringExprTimezone(f.fromString());
		assertDateToPartsExprTimezone(f.toParts(), null);
		assertDateToPartsExprTimezone(f.toIsoWeekParts(), true);
		assertDateToPartsExprTimezone(f.toParts(true), true);
		assertDateToPartsExprTimezone(f.toParts(false), false);
		assertDateExprStringTimezoneOp(f.toString(FORMAT));
		assertDateExprTimezone2Op(f.dayOfMonth(TIMEZONE2), "dayOfMonth");
		assertDateExprTimezone2Op(f.dayOfWeek(TIMEZONE2), "dayOfWeek");
		assertDateExprTimezone2Op(f.dayOfYear(TIMEZONE2), "dayOfYear");
		assertDateExprTimezone2Op(f.hour(TIMEZONE2), "hour");
		assertDateExprTimezone2Op(f.isoDayOfWeek(TIMEZONE2), "isoDayOfWeek");
		assertDateExprTimezone2Op(f.isoWeek(TIMEZONE2), "isoWeek");
		assertDateExprTimezone2Op(f.isoWeekYear(TIMEZONE2), "isoWeekYear");
		assertDateExprTimezone2Op(f.millisecond(TIMEZONE2), "millisecond");
		assertDateExprTimezone2Op(f.minute(TIMEZONE2), "minute");
		assertDateExprTimezone2Op(f.month(TIMEZONE2), "month");
		assertDateExprTimezone2Op(f.second(TIMEZONE2), "second");
		assertDateExprTimezone2Op(f.week(TIMEZONE2), "week");
		assertDateExprTimezone2Op(f.year(TIMEZONE2), "year");
		assertDateToPartsExprTimezone2(f.toParts(TIMEZONE2), null);
		assertDateToPartsExprTimezone2(f.toIsoWeekParts(TIMEZONE2), true);
		assertDateToPartsExprTimezone2(f.toParts(TIMEZONE2, true), true);
		assertDateToPartsExprTimezone2(f.toParts(TIMEZONE2, false), false);
		assertDateFromStringExprTimezone2(f.fromString(TIMEZONE2));
		assertQuarterExprStringTimezone2Op(f.quarter(TIMEZONE2));
		assertDateExprTimezone2Op(f.toString(FORMAT, TIMEZONE2));
		assertDateExprOp(f.dayOfMonth(null), "dayOfMonth");
		assertDateExprOp(f.dayOfWeek(null), "dayOfWeek");
		assertDateExprOp(f.dayOfYear(null), "dayOfYear");
		assertDateExprOp(f.hour(null), "hour");
		assertDateExprOp(f.isoDayOfWeek(null), "isoDayOfWeek");
		assertDateExprOp(f.isoWeek(null), "isoWeek");
		assertDateExprOp(f.isoWeekYear(null), "isoWeekYear");
		assertDateExprOp(f.millisecond(null), "millisecond");
		assertDateExprOp(f.minute(null), "minute");
		assertDateExprOp(f.month(null), "month");
		assertDateExprOp(f.second(null), "second");
		assertDateExprOp(f.week(null), "week");
		assertDateExprOp(f.year(null), "year");
		assertQuarterExprOp(f.quarter(null));
		assertDateFromStringExpr(f.fromString(null));
		assertDateToPartsExpr(f.toParts((String) null), null);
		assertDateToPartsExpr(f.toIsoWeekParts(null), true);
		assertDateToPartsExpr(f.toParts(null, true), true);
		assertDateToPartsExpr(f.toParts(null, false), false);
		assertDateExprStringNoTimezoneOp(f.toString(FORMAT, null));
	}

	@Test
	public void shouldRenderCurrentDateCorrectly() {

		CURRENT_DATE = new Date();
		pShouldRenderCurrentDateCorrectly(new DateFactory() {
			@Override
			public Object currentDate() {
				return CURRENT_DATE;
			}
		});
	}

	private void pShouldRenderCurrentDateCorrectly(DateFactory dateFactory) {
		pShouldRenderCurrentDateCorrectly(dateOf(dateFactory).withTimezone(TIMEZONE));
	}

	private void pShouldRenderCurrentDateCorrectly(DateOperatorFactory f) {

		assertCurrentDateTimezoneOp(f.dayOfMonth(), "dayOfMonth");
		assertCurrentDateTimezoneOp(f.dayOfWeek(), "dayOfWeek");
		assertCurrentDateTimezoneOp(f.dayOfYear(), "dayOfYear");
		assertCurrentDateTimezoneOp(f.hour(), "hour");
		assertCurrentDateTimezoneOp(f.isoDayOfWeek(), "isoDayOfWeek");
		assertCurrentDateTimezoneOp(f.isoWeek(), "isoWeek");
		assertCurrentDateTimezoneOp(f.isoWeekYear(), "isoWeekYear");
		assertCurrentDateTimezoneOp(f.millisecond(), "millisecond");
		assertCurrentDateTimezoneOp(f.minute(), "minute");
		assertCurrentDateTimezoneOp(f.month(), "month");
		assertCurrentDateTimezoneOp(f.second(), "second");
		assertCurrentDateTimezoneOp(f.week(), "week");
		assertCurrentDateTimezoneOp(f.year(), "year");
		assertCurrentDateToPartsTimezone(f.toParts(), null);
		assertCurrentDateToPartsTimezone(f.toIsoWeekParts(), true);
		assertCurrentDateToPartsTimezone(f.toParts(true), true);
		assertCurrentDateToPartsTimezone(f.toParts(false), false);
		assertQuarterCurrentDateTimezoneOp(f.quarter());
		assertCurrentDateFromStringTimezone(f.fromString());
		assertCurrentDateStringTimezoneOp(f.toString(FORMAT));
		assertCurrentDateTimezone2Op(f.dayOfMonth(TIMEZONE2), "dayOfMonth");
		assertCurrentDateTimezone2Op(f.dayOfWeek(TIMEZONE2), "dayOfWeek");
		assertCurrentDateTimezone2Op(f.dayOfYear(TIMEZONE2), "dayOfYear");
		assertCurrentDateTimezone2Op(f.hour(TIMEZONE2), "hour");
		assertCurrentDateTimezone2Op(f.isoDayOfWeek(TIMEZONE2), "isoDayOfWeek");
		assertCurrentDateTimezone2Op(f.isoWeek(TIMEZONE2), "isoWeek");
		assertCurrentDateTimezone2Op(f.isoWeekYear(TIMEZONE2), "isoWeekYear");
		assertCurrentDateTimezone2Op(f.millisecond(TIMEZONE2), "millisecond");
		assertCurrentDateTimezone2Op(f.minute(TIMEZONE2), "minute");
		assertCurrentDateTimezone2Op(f.month(TIMEZONE2), "month");
		assertCurrentDateTimezone2Op(f.second(TIMEZONE2), "second");
		assertCurrentDateTimezone2Op(f.week(TIMEZONE2), "week");
		assertCurrentDateTimezone2Op(f.year(TIMEZONE2), "year");
		assertCurrentDateToPartsTimezone2(f.toParts(TIMEZONE2), null);
		assertCurrentDateToPartsTimezone2(f.toIsoWeekParts(TIMEZONE2), true);
		assertCurrentDateToPartsTimezone2(f.toParts(TIMEZONE2, true), true);
		assertCurrentDateToPartsTimezone2(f.toParts(TIMEZONE2, false), false);
		assertQuarterCurrentDateTimezone2Op(f.quarter(TIMEZONE2));
		assertCurrentDateFromStringTimezone2(f.fromString(TIMEZONE2));
		assertCurrentDateStringTimezone2Op(f.toString(FORMAT, TIMEZONE2));
		assertCurrentDateOp(f.dayOfMonth(null), "dayOfMonth");
		assertCurrentDateOp(f.dayOfWeek(null), "dayOfWeek");
		assertCurrentDateOp(f.dayOfYear(null), "dayOfYear");
		assertCurrentDateOp(f.hour(null), "hour");
		assertCurrentDateOp(f.isoDayOfWeek(null), "isoDayOfWeek");
		assertCurrentDateOp(f.isoWeek(null), "isoWeek");
		assertCurrentDateOp(f.isoWeekYear(null), "isoWeekYear");
		assertCurrentDateOp(f.millisecond(null), "millisecond");
		assertCurrentDateOp(f.minute(null), "minute");
		assertCurrentDateOp(f.month(null), "month");
		assertCurrentDateOp(f.second(null), "second");
		assertCurrentDateOp(f.week(null), "week");
		assertCurrentDateOp(f.year(null), "year");
		assertQuarterCurrentDateOp(f.quarter(null));
		assertCurrentDateFromString(f.fromString(null));
		assertCurrentDateStringNoTimezoneOp(f.toString(FORMAT, null));
		assertCurrentDateToParts(f.toParts((String) null), null);
		assertCurrentDateToParts(f.toIsoWeekParts(null), true);
		assertCurrentDateToParts(f.toParts(null, true), true);
		assertCurrentDateToParts(f.toParts(null, false), false);
	}

	@Test
	public void shouldRenderCalendarDateFromPartsCorrectly() {

		final DBObject doc = new BasicDBObject();
		final BasicDBObject parts = new BasicDBObject();
		doc.put("$dateFromParts", parts);

		int year = 2017;
		DateFromParts.CalendarDatePartsBuilder dateFromParts = dateFromParts();
		parts.put("year", year);
		assertThat(dateFromParts.year(year).toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
		parts.put("year", "$year");
		assertThat(dateFromParts.yearOf("year").toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
		parts.put("year", LITERAL);
		assertThat(dateFromParts.yearOf(asLiteral(VAR_FIELD)).toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
		parts.put("year", year);
		dateFromParts.year(year);

		int month = 10;
		parts.put("month", month);
		assertThat(dateFromParts.month(month).toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
		parts.put("month", "$month");
		assertThat(dateFromParts.monthOf("month").toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
		parts.put("month", LITERAL);
		assertThat(dateFromParts.monthOf(asLiteral(VAR_FIELD)).toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
		parts.remove("month");
		dateFromParts.month(null);
		assertThat(dateFromParts.toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));

		int day = 8;
		parts.put("day", day);
		assertThat(dateFromParts.day(day).toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
		parts.put("day", "$day");
		assertThat(dateFromParts.dayOf("day").toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
		parts.put("day", LITERAL);
		assertThat(dateFromParts.dayOf(asLiteral(VAR_FIELD)).toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
		parts.remove("day");
		dateFromParts.day(null);
		assertThat(dateFromParts.toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));

		int hour = 9;
		parts.put("hour", hour);
		assertThat(dateFromParts.hour(hour).toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
		parts.put("hour", "$hour");
		assertThat(dateFromParts.hourOf("hour").toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
		parts.put("hour", LITERAL);
		assertThat(dateFromParts.hourOf(asLiteral(VAR_FIELD)).toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
		parts.remove("hour");
		dateFromParts.hour(null);
		assertThat(dateFromParts.toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));

		int minute = 15;
		parts.put("minute", minute);
		assertThat(dateFromParts.minute(minute).toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
		parts.put("minute", "$minute");
		assertThat(dateFromParts.minuteOf("minute").toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
		parts.put("minute", LITERAL);
		assertThat(dateFromParts.minuteOf(asLiteral(VAR_FIELD)).toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
		parts.remove("minute");
		dateFromParts.minute(null);
		assertThat(dateFromParts.toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));

		int second = 35;
		parts.put("second", second);
		assertThat(dateFromParts.second(second).toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
		parts.put("second", "$second");
		assertThat(dateFromParts.secondOf("second").toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
		parts.put("second", LITERAL);
		assertThat(dateFromParts.secondOf(asLiteral(VAR_FIELD)).toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
		parts.remove("second");
		dateFromParts.second(null);
		assertThat(dateFromParts.toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));

		int millisecond = 35;
		parts.put("millisecond", millisecond);
		assertThat(dateFromParts.millisecond(millisecond).toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
		parts.put("millisecond", "$millisecond");
		assertThat(dateFromParts.millisecondOf("millisecond").toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
		parts.put("millisecond", LITERAL);
		assertThat(dateFromParts.millisecondOf(asLiteral(VAR_FIELD)).toDate().toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(doc));
		parts.remove("millisecond");
		dateFromParts.millisecond(null);
		assertThat(dateFromParts.toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));

		String timezone = "America/New_York";
		parts.put("timezone", timezone);
		assertThat(dateFromParts.timezone(timezone).toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
		parts.put("timezone", "$timezone");
		assertThat(dateFromParts.timezoneOf("timezone").toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
		parts.put("timezone", LITERAL);
		assertThat(dateFromParts.timezoneOf(asLiteral(VAR_FIELD)).toDate().toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(doc));
		parts.remove("timezone");
		dateFromParts.timezone(null);
		assertThat(dateFromParts.toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
	}

	@Test
	public void shouldRenderIsoWeekDateFromPartsCorrectly() {

		final DBObject doc = new BasicDBObject();
		final BasicDBObject parts = new BasicDBObject();
		doc.put("$dateFromParts", parts);

		int isoWeekYear = 2017;
		DateFromParts.IsoWeekDatePartsBuilder dateFromParts = dateFromIsoWeekParts();
		parts.put("isoWeekYear", isoWeekYear);
		assertThat(dateFromParts.isoWeekYear(isoWeekYear).toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
		parts.put("isoWeekYear", "$isoWeekYear");
		assertThat(dateFromParts.isoWeekYearOf("isoWeekYear").toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
		parts.put("isoWeekYear", LITERAL);
		assertThat(dateFromParts.isoWeekYearOf(asLiteral(VAR_FIELD)).toDate().toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(doc));
		parts.put("isoWeekYear", isoWeekYear);
		dateFromParts.isoWeekYear(isoWeekYear);

		int isoWeek = 25;
		parts.put("isoWeek", isoWeek);
		assertThat(dateFromParts.isoWeek(isoWeek).toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
		parts.put("isoWeek", "$isoWeek");
		assertThat(dateFromParts.isoWeekOf("isoWeek").toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
		parts.put("isoWeek", LITERAL);
		assertThat(dateFromParts.isoWeekOf(asLiteral(VAR_FIELD)).toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
		parts.remove("isoWeek");
		dateFromParts.isoWeek(null);
		assertThat(dateFromParts.toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));

		int isoDayOfWeek = 4;
		parts.put("isoDayOfWeek", isoDayOfWeek);
		assertThat(dateFromParts.isoDayOfWeek(isoDayOfWeek).toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
		parts.put("isoDayOfWeek", "$isoDayOfWeek");
		assertThat(dateFromParts.isoDayOfWeekOf("isoDayOfWeek").toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
		parts.put("isoDayOfWeek", LITERAL);
		assertThat(dateFromParts.isoDayOfWeekOf(asLiteral(VAR_FIELD)).toDate().toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(doc));
		parts.remove("isoDayOfWeek");
		dateFromParts.isoDayOfWeek(null);
		assertThat(dateFromParts.toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));

		int hour = 9;
		parts.put("hour", hour);
		assertThat(dateFromParts.hour(hour).toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
		parts.put("hour", "$hour");
		assertThat(dateFromParts.hourOf("hour").toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
		parts.put("hour", LITERAL);
		assertThat(dateFromParts.hourOf(asLiteral(VAR_FIELD)).toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
		parts.remove("hour");
		dateFromParts.hour(null);
		assertThat(dateFromParts.toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));

		int minute = 15;
		parts.put("minute", minute);
		assertThat(dateFromParts.minute(minute).toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
		parts.put("minute", "$minute");
		assertThat(dateFromParts.minuteOf("minute").toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
		parts.put("minute", LITERAL);
		assertThat(dateFromParts.minuteOf(asLiteral(VAR_FIELD)).toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
		parts.remove("minute");
		dateFromParts.minute(null);
		assertThat(dateFromParts.toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));

		int second = 35;
		parts.put("second", second);
		assertThat(dateFromParts.second(second).toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
		parts.put("second", "$second");
		assertThat(dateFromParts.secondOf("second").toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
		parts.put("second", LITERAL);
		assertThat(dateFromParts.secondOf(asLiteral(VAR_FIELD)).toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
		parts.remove("second");
		dateFromParts.second(null);
		assertThat(dateFromParts.toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));

		int millisecond = 35;
		parts.put("millisecond", millisecond);
		assertThat(dateFromParts.millisecond(millisecond).toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
		parts.put("millisecond", "$millisecond");
		assertThat(dateFromParts.millisecondOf("millisecond").toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
		parts.put("millisecond", LITERAL);
		assertThat(dateFromParts.millisecondOf(asLiteral(VAR_FIELD)).toDate().toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(doc));
		parts.remove("millisecond");
		dateFromParts.millisecond(null);
		assertThat(dateFromParts.toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));

		String timezone = "America/New_York";
		parts.put("timezone", timezone);
		assertThat(dateFromParts.timezone(timezone).toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
		parts.put("timezone", "$timezone");
		assertThat(dateFromParts.timezoneOf("timezone").toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
		parts.put("timezone", LITERAL);
		assertThat(dateFromParts.timezoneOf(asLiteral(VAR_FIELD)).toDate().toDbObject(Aggregation.DEFAULT_CONTEXT),
				is(doc));
		parts.remove("timezone");
		dateFromParts.timezone(null);
		assertThat(dateFromParts.toDate().toDbObject(Aggregation.DEFAULT_CONTEXT), is(doc));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDateFromPartsNoCalendarYearException() {
		dateFromParts().toDate();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDateFromPartsNoIsoWeekYearException() {
		dateFromIsoWeekParts().toDate();
	}

	private void assertDateFieldOp(AggregationExpression operation, final String opName) {
		assertThat(operation.toDbObject(Aggregation.DEFAULT_CONTEXT),
				is((DBObject) new BasicDBObject(VAR + opName, VAR_FIELD)));
	}

	private void assertDateFieldTimezoneOp(AggregationExpression operation, final String opName) {

		final BasicDBObject val = new BasicDBObject();
		val.put("date", VAR_FIELD);
		val.put("timezone", TIMEZONE);
		assertThat(operation.toDbObject(Aggregation.DEFAULT_CONTEXT), is((DBObject) new BasicDBObject(VAR + opName, val)));
	}

	private void assertDateFieldTimezone2Op(AggregationExpression operation, final String opName) {

		final BasicDBObject val = new BasicDBObject();
		val.put("date", VAR_FIELD);
		val.put("timezone", TIMEZONE2);
		assertThat(operation.toDbObject(Aggregation.DEFAULT_CONTEXT), is((DBObject) new BasicDBObject(VAR + opName, val)));
	}

	private void assertDateExprOp(AggregationExpression operation, final String opName) {
		assertThat(operation.toDbObject(Aggregation.DEFAULT_CONTEXT),
				is((DBObject) new BasicDBObject(VAR + opName, LITERAL)));
	}

	private void assertDateExprTimezoneOp(AggregationExpression operation, final String opName) {

		final BasicDBObject val = new BasicDBObject();
		val.put("date", LITERAL);
		val.put("timezone", TIMEZONE);
		assertThat(operation.toDbObject(Aggregation.DEFAULT_CONTEXT), is((DBObject) new BasicDBObject(VAR + opName, val)));
	}

	private void assertDateExprTimezone2Op(AggregationExpression operation, final String opName) {

		final BasicDBObject val = new BasicDBObject();
		val.put("date", LITERAL);
		val.put("timezone", TIMEZONE2);
		assertThat(operation.toDbObject(Aggregation.DEFAULT_CONTEXT), is((DBObject) new BasicDBObject(VAR + opName, val)));
	}

	private void assertDateFieldStringNoTimezoneOp(AggregationExpression operation) {

		final BasicDBObject val = new BasicDBObject();
		val.put("date", VAR_FIELD);
		val.put("format", FORMAT);
		assertThat(operation.toDbObject(Aggregation.DEFAULT_CONTEXT), is((DBObject) new BasicDBObject(TO_STRING_OP, val)));
	}

	private void assertDateFieldStringTimezoneOp(AggregationExpression operation) {

		final BasicDBObject val = new BasicDBObject();
		val.put("date", VAR_FIELD);
		val.put("format", FORMAT);
		val.put("timezone", TIMEZONE);
		assertThat(operation.toDbObject(Aggregation.DEFAULT_CONTEXT), is((DBObject) new BasicDBObject(TO_STRING_OP, val)));
	}

	private void assertDateFieldStringTimezone2Op(AggregationExpression operation) {

		final BasicDBObject val = new BasicDBObject();
		val.put("date", VAR_FIELD);
		val.put("format", FORMAT);
		val.put("timezone", TIMEZONE2);
		assertThat(operation.toDbObject(Aggregation.DEFAULT_CONTEXT), is((DBObject) new BasicDBObject(TO_STRING_OP, val)));
	}

	private void assertDateExprStringNoTimezoneOp(AggregationExpression operation) {

		final BasicDBObject val = new BasicDBObject();
		val.put("date", LITERAL);
		val.put("format", FORMAT);
		assertThat(operation.toDbObject(Aggregation.DEFAULT_CONTEXT), is((DBObject) new BasicDBObject(TO_STRING_OP, val)));
	}

	private void assertDateExprStringTimezoneOp(AggregationExpression operation) {
		final BasicDBObject val = new BasicDBObject();
		val.put("date", LITERAL);
		val.put("format", FORMAT);
		val.put("timezone", TIMEZONE);
		assertThat(operation.toDbObject(Aggregation.DEFAULT_CONTEXT), is((DBObject) new BasicDBObject(TO_STRING_OP, val)));
	}

	private void assertDateExprTimezone2Op(AggregationExpression operation) {

		final BasicDBObject val = new BasicDBObject();
		val.put("date", LITERAL);
		val.put("format", FORMAT);
		val.put("timezone", TIMEZONE2);
		assertThat(operation.toDbObject(Aggregation.DEFAULT_CONTEXT), is((DBObject) new BasicDBObject(TO_STRING_OP, val)));
	}

	private void assertDateFromStringField(AggregationExpression operation) {

		final BasicDBObject val = new BasicDBObject();
		val.put("dateString", VAR_FIELD);
		assertThat(operation.toDbObject(Aggregation.DEFAULT_CONTEXT),
				is((DBObject) new BasicDBObject("$dateFromString", val)));
	}

	private void assertDateFromStringFieldTimezone(AggregationExpression operation) {

		final BasicDBObject val = new BasicDBObject();
		val.put("dateString", VAR_FIELD);
		val.put("timezone", TIMEZONE);
		assertThat(operation.toDbObject(Aggregation.DEFAULT_CONTEXT),
				is((DBObject) new BasicDBObject("$dateFromString", val)));
	}

	private void assertDateFromStringFieldTimezone2(AggregationExpression operation) {

		final BasicDBObject val = new BasicDBObject();
		val.put("dateString", VAR_FIELD);
		val.put("timezone", TIMEZONE2);
		assertThat(operation.toDbObject(Aggregation.DEFAULT_CONTEXT),
				is((DBObject) new BasicDBObject("$dateFromString", val)));
	}

	private void assertDateFromStringExpr(AggregationExpression operation) {

		final BasicDBObject val = new BasicDBObject();
		val.put("dateString", LITERAL);
		assertThat(operation.toDbObject(Aggregation.DEFAULT_CONTEXT),
				is((DBObject) new BasicDBObject("$dateFromString", val)));
	}

	private void assertDateFromStringExprTimezone(AggregationExpression operation) {

		final BasicDBObject val = new BasicDBObject();
		val.put("dateString", LITERAL);
		val.put("timezone", TIMEZONE);
		assertThat(operation.toDbObject(Aggregation.DEFAULT_CONTEXT),
				is((DBObject) new BasicDBObject("$dateFromString", val)));
	}

	private void assertDateFromStringExprTimezone2(AggregationExpression operation) {

		final BasicDBObject val = new BasicDBObject();
		val.put("dateString", LITERAL);
		val.put("timezone", TIMEZONE2);
		assertThat(operation.toDbObject(Aggregation.DEFAULT_CONTEXT),
				is((DBObject) new BasicDBObject("$dateFromString", val)));
	}

	private void assertCurrentDateFromString(AggregationExpression operation) {

		final BasicDBObject val = new BasicDBObject();
		val.put("dateString", CURRENT_DATE);
		assertThat(operation.toDbObject(Aggregation.DEFAULT_CONTEXT),
				is((DBObject) new BasicDBObject("$dateFromString", val)));
	}

	private void assertCurrentDateFromStringTimezone(AggregationExpression operation) {

		final BasicDBObject val = new BasicDBObject();
		val.put("dateString", CURRENT_DATE);
		val.put("timezone", TIMEZONE);
		assertThat(operation.toDbObject(Aggregation.DEFAULT_CONTEXT),
				is((DBObject) new BasicDBObject("$dateFromString", val)));
	}

	private void assertCurrentDateFromStringTimezone2(AggregationExpression operation) {

		final BasicDBObject val = new BasicDBObject();
		val.put("dateString", CURRENT_DATE);
		val.put("timezone", TIMEZONE2);
		assertThat(operation.toDbObject(Aggregation.DEFAULT_CONTEXT),
				is((DBObject) new BasicDBObject("$dateFromString", val)));
	}

	private void assertDateToParts(AggregationExpression operation, Boolean iso8601, Object dateValue,
			Object timezoneValue) {

		final BasicDBObject val = new BasicDBObject();
		val.put("date", dateValue);
		if (iso8601 != null) {
			val.put("iso8601", iso8601);
		}
		if (timezoneValue != null) {
			val.put("timezone", timezoneValue);
		}
		assertThat(operation.toDbObject(Aggregation.DEFAULT_CONTEXT),
				is((DBObject) new BasicDBObject("$dateToParts", val)));
	}

	private void assertDateToPartsField(AggregationExpression operation, Boolean iso8601) {
		assertDateToParts(operation, iso8601, VAR_FIELD, null);
	}

	private void assertDateToPartsFieldTimezone(AggregationExpression operation, Boolean iso8601) {
		assertDateToParts(operation, iso8601, VAR_FIELD, TIMEZONE);
	}

	private void assertDateToPartsFieldTimezone2(AggregationExpression operation, Boolean iso8601) {
		assertDateToParts(operation, iso8601, VAR_FIELD, TIMEZONE2);
	}

	private void assertDateToPartsExpr(AggregationExpression operation, Boolean iso8601) {
		assertDateToParts(operation, iso8601, LITERAL, null);
	}

	private void assertDateToPartsExprTimezone(AggregationExpression operation, Boolean iso8601) {
		assertDateToParts(operation, iso8601, LITERAL, TIMEZONE);
	}

	private void assertDateToPartsExprTimezone2(AggregationExpression operation, Boolean iso8601) {
		assertDateToParts(operation, iso8601, LITERAL, TIMEZONE2);
	}

	private void assertCurrentDateToParts(AggregationExpression operation, Boolean iso8601) {
		assertDateToParts(operation, iso8601, CURRENT_DATE, null);
	}

	private void assertCurrentDateToPartsTimezone(AggregationExpression operation, Boolean iso8601) {
		assertDateToParts(operation, iso8601, CURRENT_DATE, TIMEZONE);
	}

	private void assertCurrentDateToPartsTimezone2(AggregationExpression operation, Boolean iso8601) {
		assertDateToParts(operation, iso8601, CURRENT_DATE, TIMEZONE2);
	}

	private void assertQuarter(AggregationExpression operation, final BasicDBObject monthDoc) {
		final DBObject document = new BasicDBObject("$cond",
				new BasicDBObject()
						.append("if",
								new BasicDBObject("$lte",
										Lists.newArrayList(monthDoc, 3)))
						.append("then", 1).append("else",
								new BasicDBObject("$cond",
										new BasicDBObject().append("if", new BasicDBObject("$lte", Lists.newArrayList(monthDoc, 6)))
												.append("then", 2).append("else",
														new BasicDBObject("$cond",
																new BasicDBObject()
																		.append("if", new BasicDBObject("$lte", Lists.newArrayList(monthDoc, 9)))
																		.append("then", 3).append("else", 4))))));
		assertThat(operation.toDbObject(Aggregation.DEFAULT_CONTEXT), is(document));
	}

	private void assertQuarterFieldOp(AggregationExpression operation) {
		assertQuarter(operation, new BasicDBObject("$month", VAR_FIELD));
	}

	private void assertQuarterFieldTimezoneOp(AggregationExpression operation) {
		final BasicDBObject val = new BasicDBObject();
		val.put("date", VAR_FIELD);
		val.put("timezone", TIMEZONE);
		assertQuarter(operation, new BasicDBObject("$month", val));
	}

	private void assertQuarterFieldTimezone2Op(AggregationExpression operation) {
		final BasicDBObject val = new BasicDBObject();
		val.put("date", VAR_FIELD);
		val.put("timezone", TIMEZONE2);
		assertQuarter(operation, new BasicDBObject("$month", val));
	}

	private void assertQuarterExprOp(AggregationExpression operation) {
		assertQuarter(operation, new BasicDBObject("$month", LITERAL));
	}

	private void assertQuarterExprTimezoneOp(AggregationExpression operation) {
		final BasicDBObject val = new BasicDBObject();
		val.put("date", LITERAL);
		val.put("timezone", TIMEZONE);
		assertQuarter(operation, new BasicDBObject("$month", val));
	}

	private void assertQuarterExprStringTimezone2Op(AggregationExpression operation) {
		final BasicDBObject val = new BasicDBObject();
		val.put("date", LITERAL);
		val.put("timezone", TIMEZONE2);
		assertQuarter(operation, new BasicDBObject("$month", val));
	}

	private void assertQuarterCurrentDateOp(AggregationExpression operation) {
		assertQuarter(operation, new BasicDBObject("$month", CURRENT_DATE));
	}

	private void assertQuarterCurrentDateTimezoneOp(AggregationExpression operation) {
		final BasicDBObject val = new BasicDBObject();
		val.put("date", CURRENT_DATE);
		val.put("timezone", TIMEZONE);
		assertQuarter(operation, new BasicDBObject("$month", val));
	}

	private void assertQuarterCurrentDateTimezone2Op(AggregationExpression operation) {
		final BasicDBObject val = new BasicDBObject();
		val.put("date", CURRENT_DATE);
		val.put("timezone", TIMEZONE2);
		assertQuarter(operation, new BasicDBObject("$month", val));
	}

	private void assertCurrentDateOp(AggregationExpression operation, final String opName) {

		assertThat(operation.toDbObject(Aggregation.DEFAULT_CONTEXT),
				is((DBObject) new BasicDBObject(VAR + opName, CURRENT_DATE)));
	}

	private void assertCurrentDateTimezoneOp(AggregationExpression operation, final String opName) {

		final BasicDBObject val = new BasicDBObject();
		val.put("date", CURRENT_DATE);
		val.put("timezone", TIMEZONE);
		assertThat(operation.toDbObject(Aggregation.DEFAULT_CONTEXT), is((DBObject) new BasicDBObject(VAR + opName, val)));
	}

	private void assertCurrentDateTimezone2Op(AggregationExpression operation, final String opName) {

		final BasicDBObject val = new BasicDBObject();
		val.put("date", CURRENT_DATE);
		val.put("timezone", TIMEZONE2);
		assertThat(operation.toDbObject(Aggregation.DEFAULT_CONTEXT), is((DBObject) new BasicDBObject(VAR + opName, val)));
	}

	private void assertCurrentDateStringNoTimezoneOp(AggregationExpression operation) {

		final BasicDBObject val = new BasicDBObject();
		val.put("date", CURRENT_DATE);
		val.put("format", FORMAT);
		assertThat(operation.toDbObject(Aggregation.DEFAULT_CONTEXT), is((DBObject) new BasicDBObject(TO_STRING_OP, val)));
	}

	private void assertCurrentDateStringTimezoneOp(AggregationExpression operation) {

		final BasicDBObject val = new BasicDBObject();
		val.put("date", CURRENT_DATE);
		val.put("format", FORMAT);
		val.put("timezone", TIMEZONE);
		assertThat(operation.toDbObject(Aggregation.DEFAULT_CONTEXT), is((DBObject) new BasicDBObject(TO_STRING_OP, val)));
	}

	private void assertCurrentDateStringTimezone2Op(AggregationExpression operation) {

		final BasicDBObject val = new BasicDBObject();
		val.put("date", CURRENT_DATE);
		val.put("format", FORMAT);
		val.put("timezone", TIMEZONE2);
		assertThat(operation.toDbObject(Aggregation.DEFAULT_CONTEXT), is((DBObject) new BasicDBObject(TO_STRING_OP, val)));
	}

}
