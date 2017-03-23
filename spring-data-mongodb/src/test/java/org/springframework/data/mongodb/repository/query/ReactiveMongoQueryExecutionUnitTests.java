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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Method;
import java.util.Arrays;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Range;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.Person;
import org.springframework.data.mongodb.repository.query.ReactiveMongoQueryExecution.GeoNearExecution;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.util.ClassUtils;

import reactor.core.publisher.Flux;

/**
 * Unit tests for {@link ReactiveMongoQueryExecution}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class ReactiveMongoQueryExecutionUnitTests {

	@Mock private ReactiveMongoOperations operations;
	@Mock private MongoParameterAccessor parameterAccessor;

	@Test // DATAMONGO-1444
	public void geoNearExecutionShouldApplyQuerySettings() throws Exception {

		Method geoNear = ClassUtils.getMethod(GeoRepo.class, "geoNear");
		Query query = new Query();
		when(parameterAccessor.getGeoNearLocation()).thenReturn(new Point(1, 2));
		when(parameterAccessor.getDistanceRange()).thenReturn(new Range<>(new Distance(10), new Distance(15)));
		when(parameterAccessor.getPageable()).thenReturn(new PageRequest(1, 10));

		new GeoNearExecution(operations, parameterAccessor, ClassTypeInformation.fromReturnTypeOf(geoNear)).execute(query,
				Person.class, "person");

		ArgumentCaptor<NearQuery> queryArgumentCaptor = ArgumentCaptor.forClass(NearQuery.class);
		verify(operations).geoNear(queryArgumentCaptor.capture(), eq(Person.class), eq("person"));

		NearQuery nearQuery = queryArgumentCaptor.getValue();
		assertThat(nearQuery.toDocument().get("near"), is(equalTo(Arrays.asList(1d, 2d))));
		assertThat(nearQuery.getSkip(), is(10L));
		assertThat(nearQuery.getMinDistance(), is(equalTo(new Distance(10))));
		assertThat(nearQuery.getMaxDistance(), is(equalTo(new Distance(15))));
	}

	@Test // DATAMONGO-1444
	public void geoNearExecutionShouldApplyMinimalSettings() throws Exception {

		Method geoNear = ClassUtils.getMethod(GeoRepo.class, "geoNear");
		Query query = new Query();
		when(parameterAccessor.getGeoNearLocation()).thenReturn(new Point(1, 2));
		when(parameterAccessor.getDistanceRange()).thenReturn(new Range<>(null, null));

		new GeoNearExecution(operations, parameterAccessor, ClassTypeInformation.fromReturnTypeOf(geoNear)).execute(query,
				Person.class, "person");

		ArgumentCaptor<NearQuery> queryArgumentCaptor = ArgumentCaptor.forClass(NearQuery.class);
		verify(operations).geoNear(queryArgumentCaptor.capture(), eq(Person.class), eq("person"));

		NearQuery nearQuery = queryArgumentCaptor.getValue();
		assertThat(nearQuery.toDocument().get("near"), is(equalTo(Arrays.asList(1d, 2d))));
		assertThat(nearQuery.getSkip(), is(0L));
		assertThat(nearQuery.getMinDistance(), is(nullValue()));
		assertThat(nearQuery.getMaxDistance(), is(nullValue()));
	}

	interface GeoRepo {
		Flux<GeoResult<Person>> geoNear();
	}
}
