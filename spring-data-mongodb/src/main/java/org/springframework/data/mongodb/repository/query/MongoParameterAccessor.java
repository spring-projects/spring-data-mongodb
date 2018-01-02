/*
 * Copyright 2011-2018 the original author or authors.
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
package org.springframework.data.mongodb.repository.query;

import org.springframework.data.domain.Range;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.query.TextCriteria;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.lang.Nullable;

/**
 * Mongo-specific {@link ParameterAccessor} exposing a maximum distance parameter.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Mark Paluch
 */
public interface MongoParameterAccessor extends ParameterAccessor {

	/**
	 * Returns a {@link Distance} to be applied to Mongo geo queries.
	 *
	 * @return the maximum distance to apply to the geo query or {@literal null} if there's no {@link Distance} parameter
	 *         at all or the given value for it was {@literal null}.
	 */
	Range<Distance> getDistanceRange();

	/**
	 * Returns the {@link Point} to use for a geo-near query.
	 *
	 * @return
	 */
	Point getGeoNearLocation();

	/**
	 * Returns the {@link TextCriteria} to be used for full text query.
	 *
	 * @return null if not set.
	 * @since 1.6
	 */
	@Nullable
	TextCriteria getFullText();

	/**
	 * Returns the raw parameter values of the underlying query method.
	 *
	 * @return
	 * @since 1.8
	 */
	Object[] getValues();
}
