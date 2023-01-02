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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.lang.reflect.Method;
import java.util.Arrays;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reactivestreams.Publisher;

import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Range;
import org.springframework.data.domain.Range.Bound;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.Person;
import org.springframework.data.mongodb.repository.query.ReactiveMongoQueryExecution.DeleteExecution;
import org.springframework.data.mongodb.repository.query.ReactiveMongoQueryExecution.GeoNearExecution;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.util.ClassUtils;

import com.mongodb.client.result.DeleteResult;

/**
 * Unit tests for {@link ReactiveMongoQueryExecution}.
 *
 * @author Mark Paluch
 * @author Artyom Gabeev
 */
@ExtendWith(MockitoExtension.class)
public class ReactiveMongoQueryExecutionUnitTests {

	@Mock private ReactiveMongoOperations operations;
	@Mock private MongoParameterAccessor parameterAccessor;
	@Mock private MongoQueryMethod method;

	@Test // DATAMONGO-1444
	public void geoNearExecutionShouldApplyQuerySettings() throws Exception {

		Method geoNear = ClassUtils.getMethod(GeoRepo.class, "geoNear");
		Query query = new Query();
		when(parameterAccessor.getGeoNearLocation()).thenReturn(new Point(1, 2));
		when(parameterAccessor.getDistanceRange())
				.thenReturn(Range.from(Bound.inclusive(new Distance(10))).to(Bound.inclusive(new Distance(15))));
		when(parameterAccessor.getPageable()).thenReturn(PageRequest.of(1, 10));

		new GeoNearExecution(operations, parameterAccessor, ClassTypeInformation.fromReturnTypeOf(geoNear)).execute(query,
				Person.class, "person");

		ArgumentCaptor<NearQuery> queryArgumentCaptor = ArgumentCaptor.forClass(NearQuery.class);
		verify(operations).geoNear(queryArgumentCaptor.capture(), eq(Person.class), eq("person"));

		NearQuery nearQuery = queryArgumentCaptor.getValue();
		assertThat(nearQuery.toDocument().get("near")).isEqualTo(Arrays.asList(1d, 2d));
		assertThat(nearQuery.getSkip()).isEqualTo(10L);
		assertThat(nearQuery.getMinDistance()).isEqualTo(new Distance(10));
		assertThat(nearQuery.getMaxDistance()).isEqualTo(new Distance(15));
	}

	@Test // DATAMONGO-1444
	public void geoNearExecutionShouldApplyMinimalSettings() throws Exception {

		Method geoNear = ClassUtils.getMethod(GeoRepo.class, "geoNear");
		Query query = new Query();
		when(parameterAccessor.getPageable()).thenReturn(Pageable.unpaged());
		when(parameterAccessor.getGeoNearLocation()).thenReturn(new Point(1, 2));
		when(parameterAccessor.getDistanceRange()).thenReturn(Range.unbounded());

		new GeoNearExecution(operations, parameterAccessor, ClassTypeInformation.fromReturnTypeOf(geoNear)).execute(query,
				Person.class, "person");

		ArgumentCaptor<NearQuery> queryArgumentCaptor = ArgumentCaptor.forClass(NearQuery.class);
		verify(operations).geoNear(queryArgumentCaptor.capture(), eq(Person.class), eq("person"));

		NearQuery nearQuery = queryArgumentCaptor.getValue();
		assertThat(nearQuery.toDocument().get("near")).isEqualTo(Arrays.asList(1d, 2d));
		assertThat(nearQuery.getSkip()).isEqualTo(0L);
		assertThat(nearQuery.getMinDistance()).isNull();
		assertThat(nearQuery.getMaxDistance()).isNull();
	}

	@Test // DATAMONGO-2351
	public void acknowledgedDeleteReturnsDeletedCount() {

		when(operations.remove(any(Query.class), any(Class.class), anyString()))
				.thenReturn(Mono.just(DeleteResult.acknowledged(10)));

		Mono.from((Publisher<Long>) new DeleteExecution(operations, method).execute(new Query(), Class.class, "")) //
				.as(StepVerifier::create) //
				.expectNext(10L) //
				.verifyComplete();
	}

	@Test // DATAMONGO-2351
	public void unacknowledgedDeleteReturnsZeroDeletedCount() {

		when(operations.remove(any(Query.class), any(Class.class), anyString()))
				.thenReturn(Mono.just(DeleteResult.unacknowledged()));

		Mono.from((Publisher<Long>) new DeleteExecution(operations, method).execute(new Query(), Class.class, "")) //
				.as(StepVerifier::create) //
				.expectNext(0L) //
				.verifyComplete();
	}

	interface GeoRepo {
		Flux<GeoResult<Person>> geoNear();
	}
}
