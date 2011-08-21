/*
 * Copyright 2011 the original author or authors.
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
package org.springframework.data.mongodb.repository;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.lang.reflect.Method;
import java.util.List;

import org.junit.Test;
import org.springframework.data.mongodb.core.geo.Distance;
import org.springframework.data.mongodb.core.geo.Metrics;
import org.springframework.data.mongodb.core.geo.Point;

/**
 * Unit tests for {@link MongoParametersParameterAccessor}.
 * 
 * @author Oliver Gierke
 */
public class MongoParametersParameterAccessorUnitTests {

	private static final Distance DISTANCE = new Distance(2.5, Metrics.KILOMETERS);

	@Test
	public void returnsNullForDistanceIfNoneAvailable() throws NoSuchMethodException, SecurityException {

		Method method = PersonRepository.class.getMethod("findByLocationNear", Point.class);
		MongoParameters parameters = new MongoParameters(method);

		MongoParameterAccessor accessor = new MongoParametersParameterAccessor(parameters,
				new Object[] { new Point(10, 20) });
		assertThat(accessor.getMaxDistance(), is(nullValue()));
	}

	@Test
	public void returnsDistanceIfAvailable() throws NoSuchMethodException, SecurityException {

		Method method = PersonRepository.class.getMethod("findByLocationNear", Point.class, Distance.class);
		MongoParameters parameters = new MongoParameters(method);

		MongoParameterAccessor accessor = new MongoParametersParameterAccessor(parameters, new Object[] {
				new Point(10, 20), DISTANCE });
		assertThat(accessor.getMaxDistance(), is(DISTANCE));
	}

	interface PersonRepository {

		List<Person> findByLocationNear(Point point);

		List<Person> findByLocationNear(Point point, Distance distance);
	}
}
