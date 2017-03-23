/*
 * Copyright 2016-2017 the original author or authors.
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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoPage;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.Person;
import org.springframework.data.mongodb.repository.query.MongoQueryExecution.PagedExecution;
import org.springframework.data.mongodb.repository.query.MongoQueryExecution.PagingGeoNearExecution;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.util.ReflectionUtils;

/**
 * Unit tests for {@link MongoQueryExecution}.
 * 
 * @author Mark Paluch
 * @soundtrack U Can't Touch This - MC Hammer
 */
@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings("unchecked")
public class MongoQueryExecutionUnitTests {

	@Mock MongoOperations mongoOperationsMock;
	@Mock DbRefResolver dbRefResolver;

	Point POINT = new Point(10, 20);
	Distance DISTANCE = new Distance(2.5, Metrics.KILOMETERS);
	RepositoryMetadata metadata = new DefaultRepositoryMetadata(PersonRepository.class);
	MongoMappingContext context = new MongoMappingContext();
	ProjectionFactory factory = new SpelAwareProxyProjectionFactory();
	Method method = ReflectionUtils.findMethod(PersonRepository.class, "findByLocationNear", Point.class, Distance.class,
			Pageable.class);
	MongoQueryMethod queryMethod = new MongoQueryMethod(method, metadata, factory, context);

	@Before
	public void setUp() throws Exception {

		MappingMongoConverter converter = new MappingMongoConverter(dbRefResolver, context);
		when(mongoOperationsMock.getConverter()).thenReturn(converter);
	}

	@Test // DATAMONGO-1464
	public void pagedExecutionShouldNotGenerateCountQueryIfQueryReportedNoResults() {

		when(mongoOperationsMock.find(any(Query.class), eq(Person.class), eq("person")))
				.thenReturn(Collections.<Person> emptyList());

		PagedExecution execution = new PagedExecution(mongoOperationsMock, new PageRequest(0, 10));
		execution.execute(new Query(), Person.class, "person");

		verify(mongoOperationsMock).find(any(Query.class), eq(Person.class), eq("person"));
		verify(mongoOperationsMock, never()).count(any(Query.class), eq(Person.class), eq("person"));
	}

	@Test // DATAMONGO-1464
	public void pagedExecutionShouldUseCountFromResultWithOffsetAndResultsWithinPageSize() {

		when(mongoOperationsMock.find(any(Query.class), eq(Person.class), eq("person")))
				.thenReturn(Arrays.asList(new Person(), new Person(), new Person(), new Person()));

		PagedExecution execution = new PagedExecution(mongoOperationsMock, new PageRequest(0, 10));
		execution.execute(new Query(), Person.class, "person");

		verify(mongoOperationsMock).find(any(Query.class), eq(Person.class), eq("person"));
		verify(mongoOperationsMock, never()).count(any(Query.class), eq(Person.class), eq("person"));
	}

	@Test // DATAMONGO-1464
	public void pagedExecutionRetrievesObjectsForPageableOutOfRange() throws Exception {

		when(mongoOperationsMock.find(any(Query.class), eq(Person.class), eq("person")))
				.thenReturn(Collections.<Person> emptyList());

		PagedExecution execution = new PagedExecution(mongoOperationsMock, new PageRequest(2, 10));
		execution.execute(new Query(), Person.class, "person");

		verify(mongoOperationsMock).find(any(Query.class), eq(Person.class), eq("person"));
		verify(mongoOperationsMock).count(any(Query.class), eq(Person.class), eq("person"));
	}

	@Test // DATAMONGO-1464
	public void pagingGeoExecutionShouldUseCountFromResultWithOffsetAndResultsWithinPageSize() throws Exception {

		MongoParameterAccessor accessor = new MongoParametersParameterAccessor(queryMethod,
				new Object[] { POINT, DISTANCE, new PageRequest(0, 10) });

		PartTreeMongoQuery query = new PartTreeMongoQuery(queryMethod, mongoOperationsMock);
		GeoResult<Person> result = new GeoResult<Person>(new Person(), DISTANCE);

		when(mongoOperationsMock.geoNear(any(NearQuery.class), eq(Person.class), eq("person")))
				.thenReturn(new GeoResults<Person>(Arrays.asList(result, result, result, result)));

		PagingGeoNearExecution execution = new PagingGeoNearExecution(mongoOperationsMock, accessor,
				ClassTypeInformation.fromReturnTypeOf(method), query);
		execution.execute(new Query(), Person.class, "person");

		verify(mongoOperationsMock).geoNear(any(NearQuery.class), eq(Person.class), eq("person"));
		verify(mongoOperationsMock, never()).count(any(Query.class), eq("person"));
	}

	@Test // DATAMONGO-1464
	public void pagingGeoExecutionRetrievesObjectsForPageableOutOfRange() throws Exception {

		MongoParameterAccessor accessor = new MongoParametersParameterAccessor(queryMethod,
				new Object[] { POINT, DISTANCE, new PageRequest(2, 10) });

		PartTreeMongoQuery query = new PartTreeMongoQuery(queryMethod, mongoOperationsMock);

		when(mongoOperationsMock.geoNear(any(NearQuery.class), eq(Person.class), eq("person")))
				.thenReturn(new GeoResults<Person>(Collections.<GeoResult<Person>> emptyList()));

		PagingGeoNearExecution execution = new PagingGeoNearExecution(mongoOperationsMock, accessor,
				ClassTypeInformation.fromReturnTypeOf(method), query);
		execution.execute(new Query(), Person.class, "person");

		verify(mongoOperationsMock).geoNear(any(NearQuery.class), eq(Person.class), eq("person"));
		verify(mongoOperationsMock).count(any(Query.class), eq("person"));
	}

	interface PersonRepository extends Repository<Person, Long> {

		GeoPage<Person> findByLocationNear(Point point, Distance distance, Pageable pageable);
	}
}
