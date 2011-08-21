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
import org.springframework.core.MethodParameter;
import org.springframework.data.mongodb.core.geo.Distance;
import org.springframework.data.mongodb.core.geo.Point;
import org.springframework.data.mongodb.repository.MongoParameters.MongoParameter;

/**
 * Unit tests for {@link MongoParameters}.
 * 
 * @author Oliver Gierke
 */
public class MongoParametersUnitTests {

	@Test
	public void discoversDistanceParameter() throws NoSuchMethodException, SecurityException {
		Method method = PersonRepository.class.getMethod("findByLocationNear", Point.class, Distance.class);
		MongoParameters parameters = new MongoParameters(method);
		
		assertThat(parameters.getNumberOfParameters(), is(2));
		assertThat(parameters.getDistanceIndex(), is(1));
		assertThat(parameters.getBindableParameters().getNumberOfParameters(), is(1));

		MongoParameter parameter = new MongoParameters.MongoParameter(new MethodParameter(method,
				parameters.getDistanceIndex()), parameters);

		assertThat(parameter.isSpecialParameter(), is(true));
		assertThat(parameter.isBindable(), is(false));
	}

	interface PersonRepository {

		List<Person> findByLocationNear(Point point, Distance distance);
	}
}
