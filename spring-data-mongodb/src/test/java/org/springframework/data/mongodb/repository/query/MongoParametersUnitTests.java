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
package org.springframework.data.mongodb.repository.query;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.lang.reflect.Method;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.mongodb.core.geo.Distance;
import org.springframework.data.mongodb.core.geo.GeoResults;
import org.springframework.data.mongodb.core.geo.Point;
import org.springframework.data.mongodb.repository.Near;
import org.springframework.data.mongodb.repository.Person;
import org.springframework.data.mongodb.repository.query.MongoParameters;
import org.springframework.data.mongodb.repository.query.MongoQueryMethod;
import org.springframework.data.repository.query.Parameter;

/**
 * Unit tests for {@link MongoParameters}.
 * 
 * @author Oliver Gierke
 */
@RunWith(MockitoJUnitRunner.class)
public class MongoParametersUnitTests {

	@Mock
	MongoQueryMethod queryMethod;

	@Test
	public void discoversDistanceParameter() throws NoSuchMethodException, SecurityException {
		Method method = PersonRepository.class.getMethod("findByLocationNear", Point.class, Distance.class);
		MongoParameters parameters = new MongoParameters(method, false);

		assertThat(parameters.getNumberOfParameters(), is(2));
		assertThat(parameters.getDistanceIndex(), is(1));
		assertThat(parameters.getBindableParameters().getNumberOfParameters(), is(1));

		Parameter parameter = parameters.getParameter(1);

		assertThat(parameter.isSpecialParameter(), is(true));
		assertThat(parameter.isBindable(), is(false));
	}

	@Test
	public void doesNotConsiderPointAsNearForSimpleQuery() throws Exception {
		Method method = PersonRepository.class.getMethod("findByLocationNear", Point.class, Distance.class);
		MongoParameters parameters = new MongoParameters(method, false);

		assertThat(parameters.getNearIndex(), is(-1));
	}

	@Test(expected = IllegalStateException.class)
	public void rejectsMultiplePointsForGeoNearMethod() throws Exception {
		Method method = PersonRepository.class.getMethod("findByLocationNearAndOtherLocation", Point.class, Point.class);
		new MongoParameters(method, true);
	}

	@Test(expected = IllegalStateException.class)
	public void rejectsMultipleDoubleArraysForGeoNearMethod() throws Exception {
		Method method = PersonRepository.class.getMethod("invalidDoubleArrays", double[].class, double[].class);
		new MongoParameters(method, true);
	}

	@Test
	public void doesNotRejectMultiplePointsForSimpleQueryMethod() throws Exception {
		Method method = PersonRepository.class.getMethod("someOtherMethod", Point.class, Point.class);
		new MongoParameters(method, false);
	}

	@Test
	public void findsAnnotatedPointForGeoNearQuery() throws Exception {
		Method method = PersonRepository.class.getMethod("findByOtherLocationAndLocationNear", Point.class, Point.class);
		MongoParameters parameters = new MongoParameters(method, true);
		assertThat(parameters.getNearIndex(), is(1));
	}

	@Test
	public void findsAnnotatedDoubleArrayForGeoNearQuery() throws Exception {
		Method method = PersonRepository.class.getMethod("validDoubleArrays", double[].class, double[].class);
		MongoParameters parameters = new MongoParameters(method, true);
		assertThat(parameters.getNearIndex(), is(1));
	}

	interface PersonRepository {

		List<Person> findByLocationNear(Point point, Distance distance);

		GeoResults<Person> findByLocationNearAndOtherLocation(Point point, Point anotherLocation);

		GeoResults<Person> invalidDoubleArrays(double[] first, double[] second);

		List<Person> someOtherMethod(Point first, Point second);

		GeoResults<Person> findByOtherLocationAndLocationNear(Point point, @Near Point anotherLocation);

		GeoResults<Person> validDoubleArrays(double[] first, @Near double[] second);
	}
}
