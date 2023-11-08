/*
 * Copyright 2008-2023 the original author or authors.
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
package org.springframework.data.mongodb.util.json;

import static java.time.format.DateTimeFormatter.*;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

/**
 * DateTimeFormatter implementation borrowed from <a href=
 * "https://github.com/mongodb/mongo-java-driver/blob/master/bson/src/main/org/bson/json/DateTimeFormatter.java">MongoDB
 * Inc.</a> licensed under the Apache License, Version 2.0. <br />
 * Formatted and modified.
 *
 * @author Jeff Yemin
 * @author Ross Lawley
 * @since 2.2
 */
class DateTimeFormatter {

	private static final int DATE_STRING_LENGTH = "1970-01-01".length();

	static long parse(final String dateTimeString) {
		// ISO_OFFSET_DATE_TIME will not parse date strings consisting of just year-month-day, so use ISO_LOCAL_DATE for
		// those
		if (dateTimeString.length() == DATE_STRING_LENGTH) {
			return LocalDate.parse(dateTimeString, ISO_LOCAL_DATE).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
		} else {
			return ISO_OFFSET_DATE_TIME.parse(dateTimeString, Instant::from).toEpochMilli();
		}
	}

	static String format(final long dateTime) {
		return ZonedDateTime.ofInstant(Instant.ofEpochMilli(dateTime), ZoneId.of("Z")).format(ISO_OFFSET_DATE_TIME);
	}

	private DateTimeFormatter() {
	}
}
