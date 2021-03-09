/*
 * Copyright 2011-2021 the original author or authors.
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
package org.springframework.data.mongodb.repository.query;

import static org.assertj.core.api.Assertions.*;

import java.lang.reflect.Method;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Range;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.TextCriteria;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.data.mongodb.repository.Near;
import org.springframework.data.mongodb.repository.Person;
import org.springframework.data.repository.query.Parameter;

/**
 * Unit tests for {@link MongoParameters}.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
@ExtendWith(MockitoExtension.class)
class MongoParametersUnitTests {

	@Mock MongoQueryMethod queryMethod;

	@Test
	void discoversDistanceParameter() throws NoSuchMethodException, SecurityException {

		Method method = PersonRepository.class.getMethod("findByLocationNear", Point.class, Distance.class);
		MongoParameters parameters = new MongoParameters(method, false);

		assertThat(parameters.getNumberOfParameters()).isEqualTo(2);
		assertThat(parameters.getMaxDistanceIndex()).isEqualTo(1);
		assertThat(parameters.getBindableParameters().getNumberOfParameters()).isOne();

		Parameter parameter = parameters.getParameter(1);

		assertThat(parameter.isSpecialParameter()).isTrue();
		assertThat(parameter.isBindable()).isFalse();
	}

	@Test
	void doesNotConsiderPointAsNearForSimpleQuery() throws Exception {
		Method method = PersonRepository.class.getMethod("findByLocationNear", Point.class, Distance.class);
		MongoParameters parameters = new MongoParameters(method, false);

		assertThat(parameters.getNearIndex()).isEqualTo(-1);
	}

	@Test
	void rejectsMultiplePointsForGeoNearMethod() throws Exception {

		Method method = PersonRepository.class.getMethod("findByLocationNearAndOtherLocation", Point.class, Point.class);

		assertThatIllegalStateException().isThrownBy(() -> new MongoParameters(method, true));
	}

	@Test
	void rejectsMultipleDoubleArraysForGeoNearMethod() throws Exception {

		Method method = PersonRepository.class.getMethod("invalidDoubleArrays", double[].class, double[].class);

		assertThatIllegalStateException().isThrownBy(() -> new MongoParameters(method, true));
	}

	@Test
	void doesNotRejectMultiplePointsForSimpleQueryMethod() throws Exception {

		Method method = PersonRepository.class.getMethod("someOtherMethod", Point.class, Point.class);
		new MongoParameters(method, false);
	}

	@Test
	void findsAnnotatedPointForGeoNearQuery() throws Exception {

		Method method = PersonRepository.class.getMethod("findByOtherLocationAndLocationNear", Point.class, Point.class);
		MongoParameters parameters = new MongoParameters(method, true);
		assertThat(parameters.getNearIndex()).isOne();
	}

	@Test
	void findsAnnotatedDoubleArrayForGeoNearQuery() throws Exception {

		Method method = PersonRepository.class.getMethod("validDoubleArrays", double[].class, double[].class);
		MongoParameters parameters = new MongoParameters(method, true);
		assertThat(parameters.getNearIndex()).isOne();
	}

	@Test // DATAMONGO-973
	void shouldFindTextCriteriaAtItsIndex() throws SecurityException, NoSuchMethodException {

		Method method = PersonRepository.class.getMethod("findByNameAndText", String.class, TextCriteria.class);
		MongoParameters parameters = new MongoParameters(method, false);
		assertThat(parameters.getFullTextParameterIndex()).isOne();
	}

	@Test // DATAMONGO-973
	void shouldTreatTextCriteriaParameterAsSpecialParameter() throws SecurityException, NoSuchMethodException {

		Method method = PersonRepository.class.getMethod("findByNameAndText", String.class, TextCriteria.class);
		MongoParameters parameters = new MongoParameters(method, false);
		assertThat(parameters.getParameter(parameters.getFullTextParameterIndex()).isSpecialParameter()).isTrue();
	}

	@Test // DATAMONGO-1110
	void shouldFindMinAndMaxDistanceParameters() throws NoSuchMethodException, SecurityException {

		Method method = PersonRepository.class.getMethod("findByLocationNear", Point.class, Range.class);
		MongoParameters parameters = new MongoParameters(method, false);

		assertThat(parameters.getRangeIndex()).isOne();
		assertThat(parameters.getMaxDistanceIndex()).isEqualTo(-1);
	}

	@Test // DATAMONGO-1110
	void shouldNotHaveMinDistanceIfOnlyOneDistanceParameterPresent() throws NoSuchMethodException, SecurityException {

		Method method = PersonRepository.class.getMethod("findByLocationNear", Point.class, Distance.class);
		MongoParameters parameters = new MongoParameters(method, false);

		assertThat(parameters.getRangeIndex()).isEqualTo(-1);
		assertThat(parameters.getMaxDistanceIndex()).isOne();
	}

	@Test // DATAMONGO-1854
	void shouldReturnMinusOneIfCollationParameterDoesNotExist() throws NoSuchMethodException, SecurityException {

		Method method = PersonRepository.class.getMethod("findByLocationNear", Point.class, Distance.class);
		MongoParameters parameters = new MongoParameters(method, false);

		assertThat(parameters.getCollationParameterIndex()).isEqualTo(-1);
	}

	@Test // DATAMONGO-1854
	void shouldReturnIndexOfCollationParameterIfExists() throws NoSuchMethodException, SecurityException {

		Method method = PersonRepository.class.getMethod("findByText", String.class, Collation.class);
		MongoParameters parameters = new MongoParameters(method, false);

		assertThat(parameters.getCollationParameterIndex()).isOne();
	}

	@Test // GH-2107
	void shouldReturnIndexUpdateIfExists() throws NoSuchMethodException, SecurityException {

		Method method = PersonRepository.class.getMethod("findAndModifyByFirstname", String.class, UpdateDefinition.class, Pageable.class);
		MongoParameters parameters = new MongoParameters(method, false);

		assertThat(parameters.getUpdateIndex()).isOne();
	}

	@Test // GH-2107
	void shouldReturnInvalidIndexIfUpdateDoesNotExist() throws NoSuchMethodException, SecurityException {

		Method method = PersonRepository.class.getMethod("someOtherMethod", Point.class, Point.class);
		MongoParameters parameters = new MongoParameters(method, false);

		assertThat(parameters.getUpdateIndex()).isEqualTo(-1);
	}

	interface PersonRepository {

		List<Person> findByLocationNear(Point point, Distance distance);

		GeoResults<Person> findByLocationNearAndOtherLocation(Point point, Point anotherLocation);

		GeoResults<Person> invalidDoubleArrays(double[] first, double[] second);

		List<Person> someOtherMethod(Point first, Point second);

		GeoResults<Person> findByOtherLocationAndLocationNear(Point point, @Near Point anotherLocation);

		GeoResults<Person> validDoubleArrays(double[] first, @Near double[] second);

		List<Person> findByNameAndText(String name, TextCriteria text);

		List<Person> findByLocationNear(Point point, Range<Distance> range);

		List<Person> findByText(String text, Collation collation);

		List<Person> findAndModifyByFirstname(String firstname, UpdateDefinition update, Pageable page);
	}
}
