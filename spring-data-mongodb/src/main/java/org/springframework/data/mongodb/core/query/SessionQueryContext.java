/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.mongodb.core.query;

import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.InvalidMongoDbApiUsageException;
import org.springframework.util.ObjectUtils;

/**
 * {@link QueryContext} implementation for usage inside of a MongoDB session.
 *
 * @author Christoph Strobl
 * @since 2.1
 */
enum SessionQueryContext implements QueryContext {

	INSTANCE;

	/**
	 * @return singleton instance of {@link SessionQueryContext}.
	 */
	static SessionQueryContext sessionContext() {
		return INSTANCE;
	}

	@Override
	public MongoCriteriaOperator getMongoOperator(RawCriteria criteria, String operator) {

		if (operator.equals("$maxDistance") || operator.equals("$near") || operator.equals("$nearSphere")) {

			String geoOperator = operator;
			if (geoOperator.equals("$maxDistance")) {
				geoOperator = criteria.contains("$near") ? "$near" : "$nearSphere";
			}

			return processNearQuery(criteria, geoOperator);
		}

		return QueryContext.super.getMongoOperator(criteria, operator);
	}

	private MongoCriteriaOperator processNearQuery(RawCriteria criteria, String geoOperator) {

		if (criteria.contains("$minDistance")) {
			throw new InvalidMongoDbApiUsageException(
					"$near and $nearSphere operators are not supported for count inside a MongoDB Session (Please see 'https://jira.mongodb.org/browse/DRIVERS-518' for details).\n We've tried to rewrite the Query to use $geoWithin, but found $minDistance which is not supported by this command. Please consult the MongoDB documentation for potential workarounds. Thank you!");
		}

		criteria.markProcessed(geoOperator);

		boolean spheric = geoOperator.equals("$nearSphere");
		Number distance = Double.MAX_VALUE;

		if (criteria.contains("$maxDistance")) {

			criteria.markProcessed("$maxDistance");
			distance = (Number) criteria.valueOf("$maxDistance");
		}

		List<Object> $center = Arrays.asList(toCenterCoordinates(criteria.valueOf(geoOperator)), distance);
		return new MongoCriteriaOperator("$geoWithin", new Document(spheric ? "$centerSphere" : "$center", $center));
	}

	private Object toCenterCoordinates(Object value) {

		if (ObjectUtils.isArray(value)) {
			return value;
		}

		if (value instanceof Point) {
			return Arrays.asList(((Point) value).getX(), ((Point) value).getY());
		}

		return value;
	}
}
