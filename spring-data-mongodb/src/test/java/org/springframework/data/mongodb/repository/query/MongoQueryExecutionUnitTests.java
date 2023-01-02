/*
 * Copyright 2016-2023 the original author or authors.
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
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoPage;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.ExecutableFindOperation.ExecutableFind;
import org.springframework.data.mongodb.core.ExecutableFindOperation.FindWithQuery;
import org.springframework.data.mongodb.core.ExecutableFindOperation.TerminatingFind;
import org.springframework.data.mongodb.core.ExecutableFindOperation.TerminatingFindNear;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.Person;
import org.springframework.data.mongodb.repository.query.MongoQueryExecution.DeleteExecution;
import org.springframework.data.mongodb.repository.query.MongoQueryExecution.PagedExecution;
import org.springframework.data.mongodb.repository.query.MongoQueryExecution.PagingGeoNearExecution;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.ReflectionUtils;

import com.mongodb.client.result.DeleteResult;

/**
 * Unit tests for {@link MongoQueryExecution}.
 *
 * @author Mark Paluch
 * @author Oliver Gierke
 * @author Artyom Gabeev
 * @author Christoph Strobl
 * @soundtrack U Can't Touch This - MC Hammer
 */
@ExtendWith(MockitoExtension.class)
class MongoQueryExecutionUnitTests {

	@Mock MongoOperations mongoOperationsMock;
	@Mock ExecutableFind<Object> findOperationMock;
	@Mock FindWithQuery<Object> operationMock;
	@Mock TerminatingFind<Object> terminatingMock;
	@Mock TerminatingFindNear<Object> terminatingGeoMock;
	@Mock DbRefResolver dbRefResolver;

	private SpelExpressionParser EXPRESSION_PARSER = new SpelExpressionParser();
	private Point POINT = new Point(10, 20);
	private Distance DISTANCE = new Distance(2.5, Metrics.KILOMETERS);
	private RepositoryMetadata metadata = new DefaultRepositoryMetadata(PersonRepository.class);
	private MongoMappingContext context = new MongoMappingContext();
	private ProjectionFactory factory = new SpelAwareProxyProjectionFactory();
	private Method method = ReflectionUtils.findMethod(PersonRepository.class, "findByLocationNear", Point.class,
			Distance.class,
			Pageable.class);
	private MongoQueryMethod queryMethod = new MongoQueryMethod(method, metadata, factory, context);
	private MappingMongoConverter converter;

	@BeforeEach
	@SuppressWarnings("unchecked")
	void setUp() {

		converter = new MappingMongoConverter(dbRefResolver, context);

	}

	@Test // DATAMONGO-1464
	void pagedExecutionShouldNotGenerateCountQueryIfQueryReportedNoResults() {

		doReturn(terminatingMock).when(operationMock).matching(any(Query.class));
		doReturn(Collections.emptyList()).when(terminatingMock).all();

		PagedExecution execution = new PagedExecution(operationMock, PageRequest.of(0, 10));
		execution.execute(new Query());

		verify(terminatingMock).all();
		verify(terminatingMock, never()).count();
	}

	@Test // DATAMONGO-1464
	void pagedExecutionShouldUseCountFromResultWithOffsetAndResultsWithinPageSize() {

		doReturn(terminatingMock).when(operationMock).matching(any(Query.class));
		doReturn(Arrays.asList(new Person(), new Person(), new Person(), new Person())).when(terminatingMock).all();

		PagedExecution execution = new PagedExecution(operationMock, PageRequest.of(0, 10));
		execution.execute(new Query());

		verify(terminatingMock).all();
		verify(terminatingMock, never()).count();
	}

	@Test // DATAMONGO-1464
	void pagedExecutionRetrievesObjectsForPageableOutOfRange() {

		doReturn(terminatingMock).when(operationMock).matching(any(Query.class));
		doReturn(Collections.emptyList()).when(terminatingMock).all();

		PagedExecution execution = new PagedExecution(operationMock, PageRequest.of(2, 10));
		execution.execute(new Query());

		verify(terminatingMock).all();
		verify(terminatingMock).count();
	}

	@Test // DATAMONGO-1464
	void pagingGeoExecutionShouldUseCountFromResultWithOffsetAndResultsWithinPageSize() {

		GeoResult<Person> result = new GeoResult<>(new Person(), DISTANCE);
		when(mongoOperationsMock.getConverter()).thenReturn(converter);
		when(mongoOperationsMock.query(any(Class.class))).thenReturn(findOperationMock);
		when(findOperationMock.near(any(NearQuery.class))).thenReturn(terminatingGeoMock);
		doReturn(new GeoResults<>(Arrays.asList(result, result, result, result))).when(terminatingGeoMock).all();

		ConvertingParameterAccessor accessor = new ConvertingParameterAccessor(converter,
				new MongoParametersParameterAccessor(queryMethod, new Object[] { POINT, DISTANCE, PageRequest.of(0, 10) }));

		PartTreeMongoQuery query = new PartTreeMongoQuery(queryMethod, mongoOperationsMock, EXPRESSION_PARSER,
				QueryMethodEvaluationContextProvider.DEFAULT);

		PagingGeoNearExecution execution = new PagingGeoNearExecution(findOperationMock, queryMethod, accessor, query);
		execution.execute(new Query());

		verify(terminatingGeoMock).all();
	}

	@Test // DATAMONGO-1464
	void pagingGeoExecutionRetrievesObjectsForPageableOutOfRange() {

		when(mongoOperationsMock.getConverter()).thenReturn(converter);
		when(mongoOperationsMock.query(any(Class.class))).thenReturn(findOperationMock);
		when(findOperationMock.near(any(NearQuery.class))).thenReturn(terminatingGeoMock);
		doReturn(new GeoResults<>(Collections.emptyList())).when(terminatingGeoMock).all();
		doReturn(terminatingMock).when(findOperationMock).matching(any(Query.class));

		ConvertingParameterAccessor accessor = new ConvertingParameterAccessor(converter,
				new MongoParametersParameterAccessor(queryMethod, new Object[] { POINT, DISTANCE, PageRequest.of(2, 10) }));

		PartTreeMongoQuery query = new PartTreeMongoQuery(queryMethod, mongoOperationsMock, EXPRESSION_PARSER,
				QueryMethodEvaluationContextProvider.DEFAULT);

		PagingGeoNearExecution execution = new PagingGeoNearExecution(findOperationMock, queryMethod, accessor, query);
		execution.execute(new Query());

		verify(terminatingGeoMock).all();
		verify(terminatingMock).count();
	}

	@Test // DATAMONGO-2351
	void acknowledgedDeleteReturnsDeletedCount() {

		Method method = ReflectionUtils.findMethod(PersonRepository.class, "deleteAllByLastname", String.class);
		MongoQueryMethod queryMethod = new MongoQueryMethod(method, metadata, factory, context);

		when(mongoOperationsMock.remove(any(Query.class), any(Class.class), anyString()))
				.thenReturn(DeleteResult.acknowledged(10));

		assertThat(new DeleteExecution(mongoOperationsMock, queryMethod).execute(new Query())).isEqualTo(10L);
	}

	@Test // DATAMONGO-2351
	void unacknowledgedDeleteReturnsZeroDeletedCount() {

		Method method = ReflectionUtils.findMethod(PersonRepository.class, "deleteAllByLastname", String.class);
		MongoQueryMethod queryMethod = new MongoQueryMethod(method, metadata, factory, context);

		when(mongoOperationsMock.remove(any(Query.class), any(Class.class), anyString()))
				.thenReturn(DeleteResult.unacknowledged());

		assertThat(new DeleteExecution(mongoOperationsMock, queryMethod).execute(new Query())).isEqualTo(0L);
	}

	@Test // DATAMONGO-1997
	void deleteExecutionWithEntityReturnTypeTriggersFindAndRemove() {

		Method method = ReflectionUtils.findMethod(PersonRepository.class, "deleteByLastname", String.class);
		MongoQueryMethod queryMethod = new MongoQueryMethod(method, metadata, factory, context);

		Person person = new Person();

		when(mongoOperationsMock.findAndRemove(any(Query.class), any(Class.class), anyString())).thenReturn(person);

		assertThat(new DeleteExecution(mongoOperationsMock, queryMethod).execute(new Query())).isEqualTo(person);
	}

	interface PersonRepository extends Repository<Person, Long> {

		GeoPage<Person> findByLocationNear(Point point, Distance distance, Pageable pageable);

		Long deleteAllByLastname(String lastname);

		Person deleteByLastname(String lastname);
	}
}
