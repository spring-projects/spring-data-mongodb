/*
 * Copyright 2018. the original author or authors.
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

import java.util.Date;

/**
 * Used for {@link DateOperators} related functions to access the current date
 * 
 * @since 2.1
 * @author Matt Morrissette
 */
@FunctionalInterface
public interface DateFactory {

	/**
	 * @author Matt Morrissette
	 * @param currentDate
	 * @return A date factory that always uses the given date as the current date. Primary used in testing and mock
	 *         scenarios.
	 */
	public static DateFactory fixedDate(final Object currentDate) {
		return () -> currentDate;
	}

	/**
	 * DateFactory that uses the date as it is on the local server
	 */
	public static final DateFactory LOCAL_DATE_FACTORY = Date::new;

	/**
	 * Should return an object that is serializable by the BSON encoder and would resolve to a BSON Date when evaluated.
	 * <p>
	 * This includes
	 * <ul>
	 * <li>{@link java.util.Date}</li>
	 * <li>{@link java.util.Calendar}</li>
	 * <li>{@link java.time.Instant}</li>
	 * <li>{@link java.time.ZonedDateTime}</li>
	 * <li>{@link java.lang.Long}</li>
	 * <li>org.joda.time.AbstractInstant</li>
	 * </ul>
	 *
	 * @author Matt Morrissette
	 * @return
	 */
	public Object currentDate();

}
