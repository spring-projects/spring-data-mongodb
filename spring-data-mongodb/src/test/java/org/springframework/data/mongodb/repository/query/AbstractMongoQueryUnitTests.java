/*
 * Copyright 2014-2017 the original author or authors.
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
import java.util.Date;
import java.util.List;
import java.util.Optional;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.hamcrest.core.Is;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.Person;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.mapping.BasicMongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.Meta;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;

import com.mongodb.WriteResult;
import com.mongodb.client.result.DeleteResult;

/**
 * Unit tests for {@link AbstractMongoQuery}.
 * 
 * @author Christoph Strobl
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class AbstractMongoQueryUnitTests {

	@Mock MongoOperations mongoOperationsMock;
	@Mock BasicMongoPersistentEntity<?> persitentEntityMock;
	@Mock MongoMappingContext mappingContextMock;
	@Mock WriteResult writeResultMock;
	@Mock DeleteResult deleteResultMock;

	@Before
	public void setUp() {

		doReturn("persons").when(persitentEntityMock).getCollection();
		doReturn(Optional.of(persitentEntityMock)).when(mappingContextMock).getPersistentEntity(Mockito.any(Class.class));
		doReturn(persitentEntityMock).when(mappingContextMock).getRequiredPersistentEntity(Mockito.any(Class.class));
		doReturn(Person.class).when(persitentEntityMock).getType();

		DbRefResolver dbRefResolver = new DefaultDbRefResolver(mock(MongoDbFactory.class));
		MappingMongoConverter converter = new MappingMongoConverter(dbRefResolver, mappingContextMock);
		converter.afterPropertiesSet();

		doReturn(converter).when(mongoOperationsMock).getConverter();
	}

	@SuppressWarnings("unchecked")
	@Test // DATAMONGO-566
	public void testDeleteExecutionCallsRemoveCorreclty() {

		createQueryForMethod("deletePersonByLastname", String.class).setDeleteQuery(true).execute(new Object[] { "booh" });

		verify(mongoOperationsMock, times(1)).remove(Matchers.any(Query.class), eq(Person.class), eq("persons"));
		verify(mongoOperationsMock, times(0)).find(Matchers.any(Query.class), Matchers.any(Class.class),
				Matchers.anyString());
	}

	@SuppressWarnings("unchecked")
	@Test // DATAMONGO-566, DATAMONGO-1040
	public void testDeleteExecutionLoadsListOfRemovedDocumentsWhenReturnTypeIsCollectionLike() {

		createQueryForMethod("deleteByLastname", String.class).setDeleteQuery(true).execute(new Object[] { "booh" });

		verify(mongoOperationsMock, times(1)).findAllAndRemove(Mockito.any(Query.class), eq(Person.class), eq("persons"));
	}

	@Test // DATAMONGO-566
	public void testDeleteExecutionReturnsZeroWhenWriteResultIsNull() {

		MongoQueryFake query = createQueryForMethod("deletePersonByLastname", String.class);
		query.setDeleteQuery(true);

		assertThat(query.execute(new Object[] { "fake" }), Is.<Object> is(0L));
	}

	@Test // DATAMONGO-566, DATAMONGO-978
	public void testDeleteExecutionReturnsNrDocumentsDeletedFromWriteResult() {

		when(deleteResultMock.getDeletedCount()).thenReturn(100L);
		when(mongoOperationsMock.remove(Matchers.any(Query.class), eq(Person.class), eq("persons")))
				.thenReturn(deleteResultMock);

		MongoQueryFake query = createQueryForMethod("deletePersonByLastname", String.class);
		query.setDeleteQuery(true);

		assertThat(query.execute(new Object[] { "fake" }), is((Object) 100L));
		verify(mongoOperationsMock, times(1)).remove(Matchers.any(Query.class), eq(Person.class), eq("persons"));
	}

	@Test // DATAMONGO-957
	public void metadataShouldNotBeAddedToQueryWhenNotPresent() {

		MongoQueryFake query = createQueryForMethod("findByFirstname", String.class);
		query.execute(new Object[] { "fake" });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);

		verify(mongoOperationsMock, times(1)).find(captor.capture(), eq(Person.class), eq("persons"));

		assertThat(captor.getValue().getMeta().getComment(), nullValue());
	}

	@Test // DATAMONGO-957
	public void metadataShouldBeAddedToQueryCorrectly() {

		MongoQueryFake query = createQueryForMethod("findByFirstname", String.class, Pageable.class);
		query.execute(new Object[] { "fake", new PageRequest(0, 10) });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);

		verify(this.mongoOperationsMock, times(1)).find(captor.capture(), eq(Person.class), eq("persons"));
		assertThat(captor.getValue().getMeta().getComment(), is("comment"));
	}

	@Test // DATAMONGO-957
	public void metadataShouldBeAddedToCountQueryCorrectly() {

		MongoQueryFake query = createQueryForMethod("findByFirstname", String.class, Pageable.class);
		query.execute(new Object[] { "fake", new PageRequest(1, 10) });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);

		verify(mongoOperationsMock, times(1)).count(captor.capture(), eq(Person.class), eq("persons"));
		assertThat(captor.getValue().getMeta().getComment(), is("comment"));
	}

	@Test // DATAMONGO-957
	public void metadataShouldBeAddedToStringBasedQueryCorrectly() {

		MongoQueryFake query = createQueryForMethod("findByAnnotatedQuery", String.class, Pageable.class);
		query.execute(new Object[] { "fake", new PageRequest(0, 10) });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);

		verify(this.mongoOperationsMock, times(1)).find(captor.capture(), eq(Person.class), eq("persons"));
		assertThat(captor.getValue().getMeta().getComment(), is("comment"));
	}

	@Test // DATAMONGO-1057
	public void slicedExecutionShouldRetainNrOfElementsToSkip() {

		MongoQueryFake query = createQueryForMethod("findByLastname", String.class, Pageable.class);
		Pageable page1 = new PageRequest(0, 10);
		Pageable page2 = page1.next();

		query.execute(new Object[] { "fake", page1 });
		query.execute(new Object[] { "fake", page2 });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);

		verify(mongoOperationsMock, times(2)).find(captor.capture(), eq(Person.class), eq("persons"));

		assertThat(captor.getAllValues().get(0).getSkip(), is(0L));
		assertThat(captor.getAllValues().get(1).getSkip(), is(10L));
	}

	@Test // DATAMONGO-1057
	public void slicedExecutionShouldIncrementLimitByOne() {

		MongoQueryFake query = createQueryForMethod("findByLastname", String.class, Pageable.class);
		Pageable page1 = new PageRequest(0, 10);
		Pageable page2 = page1.next();

		query.execute(new Object[] { "fake", page1 });
		query.execute(new Object[] { "fake", page2 });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);

		verify(mongoOperationsMock, times(2)).find(captor.capture(), eq(Person.class), eq("persons"));

		assertThat(captor.getAllValues().get(0).getLimit(), is(11));
		assertThat(captor.getAllValues().get(1).getLimit(), is(11));
	}

	@Test // DATAMONGO-1057
	public void slicedExecutionShouldRetainSort() {

		MongoQueryFake query = createQueryForMethod("findByLastname", String.class, Pageable.class);
		Pageable page1 = new PageRequest(0, 10, Sort.Direction.DESC, "bar");
		Pageable page2 = page1.next();

		query.execute(new Object[] { "fake", page1 });
		query.execute(new Object[] { "fake", page2 });

		ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);

		verify(mongoOperationsMock, times(2)).find(captor.capture(), eq(Person.class), eq("persons"));

		Document expectedSortObject = new Document().append("bar", -1);
		assertThat(captor.getAllValues().get(0).getSortObject(), is(expectedSortObject));
		assertThat(captor.getAllValues().get(1).getSortObject(), is(expectedSortObject));
	}

	@Test // DATAMONGO-1080
	public void doesNotTryToPostProcessQueryResultIntoWrapperType() {

		Person reference = new Person();
		when(mongoOperationsMock.findOne(Mockito.any(Query.class), eq(Person.class), eq("persons"))).//
				thenReturn(reference);

		AbstractMongoQuery query = createQueryForMethod("findByLastname", String.class);

		assertThat(query.execute(new Object[] { "lastname" }), is((Object) reference));
	}

	private MongoQueryFake createQueryForMethod(String methodName, Class<?>... paramTypes) {

		try {

			Method method = Repo.class.getMethod(methodName, paramTypes);
			ProjectionFactory factory = new SpelAwareProxyProjectionFactory();
			MongoQueryMethod queryMethod = new MongoQueryMethod(method, new DefaultRepositoryMetadata(Repo.class), factory,
					mappingContextMock);

			return new MongoQueryFake(queryMethod, mongoOperationsMock);

		} catch (NoSuchMethodException e) {
			throw new IllegalArgumentException(e.getMessage(), e);
		} catch (SecurityException e) {
			throw new IllegalArgumentException(e.getMessage(), e);
		}
	}

	private static class MongoQueryFake extends AbstractMongoQuery {

		private boolean isCountQuery;
		private boolean isExistsQuery;
		private boolean isDeleteQuery;

		public MongoQueryFake(MongoQueryMethod method, MongoOperations operations) {
			super(method, operations);
		}

		@Override
		protected Query createQuery(ConvertingParameterAccessor accessor) {
			return new BasicQuery("{'foo':'bar'}");
		}

		@Override
		protected boolean isCountQuery() {
			return isCountQuery;
		}

		@Override
		protected boolean isExistsQuery() {
			return false;
		}

		@Override
		protected boolean isDeleteQuery() {
			return isDeleteQuery;
		}

		public MongoQueryFake setDeleteQuery(boolean isDeleteQuery) {
			this.isDeleteQuery = isDeleteQuery;
			return this;
		}
	}

	private interface Repo extends MongoRepository<Person, Long> {

		List<Person> deleteByLastname(String lastname);

		Long deletePersonByLastname(String lastname);

		List<Person> findByFirstname(String firstname);

		@Meta(comment = "comment", flags = {org.springframework.data.mongodb.core.query.Meta.CursorOption.NO_TIMEOUT})
		Page<Person> findByFirstname(String firstnanme, Pageable pageable);

		@Meta(comment = "comment")
		@org.springframework.data.mongodb.repository.Query("{}")
		Page<Person> findByAnnotatedQuery(String firstnanme, Pageable pageable);

		// DATAMONGO-1057
		Slice<Person> findByLastname(String lastname, Pageable page);

		Optional<Person> findByLastname(String lastname);
	}
}
