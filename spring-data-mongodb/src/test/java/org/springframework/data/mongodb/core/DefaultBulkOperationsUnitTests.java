/*
 * Copyright 2017 the original author or authors.
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
package org.springframework.data.mongodb.core;

import static org.hamcrest.MatcherAssert.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;
import static org.springframework.data.mongodb.test.util.IsBsonObject.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.BulkOperations.BulkMode;
import org.springframework.data.mongodb.core.DefaultBulkOperations.BulkOperationContext;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.convert.UpdateMapper;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Update;

import com.mongodb.BulkUpdateRequestBuilder;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteRequestBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

/**
 * Unit tests for {@link DefaultBulkOperations}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultBulkOperationsUnitTests {

	@Mock MongoTemplate template;
	@Mock DBCollection collection;
	@Mock DbRefResolver dbRefResolver;
	@Captor ArgumentCaptor<DBObject> captor;
	@Mock BulkWriteOperation bulk;
	@Mock BulkWriteRequestBuilder bulkWriteRequestBuilder;
	@Mock BulkUpdateRequestBuilder bulkUpdateRequestBuilder;
	MongoConverter converter;
	MongoMappingContext mappingContext;

	DefaultBulkOperations ops;

	@Before
	public void before() throws Exception {

		when(bulk.find(any(DBObject.class))).thenReturn(bulkWriteRequestBuilder);
		when(bulkWriteRequestBuilder.upsert()).thenReturn(bulkUpdateRequestBuilder);
		when(collection.initializeOrderedBulkOperation()).thenReturn(bulk);

		mappingContext = new MongoMappingContext();
		mappingContext.afterPropertiesSet();

		converter = new MappingMongoConverter(dbRefResolver, mappingContext);

		when(template.getCollection(anyString())).thenReturn(collection);

		ops = new DefaultBulkOperations(template, "collection-1",
				new BulkOperationContext(BulkMode.ORDERED, mappingContext.getPersistentEntity(SomeDomainType.class),
						new QueryMapper(converter), new UpdateMapper(converter)));
	}

	@Test // DATAMONGO-1678
	public void updateOneShouldMapQueryAndUpdate() {

		ops.updateOne(new BasicQuery("{firstName:1}"), new Update().set("lastName", "targaryen")).execute();

		verify(bulk).find(captor.capture());
		verify(bulkWriteRequestBuilder).updateOne(captor.capture());

		assertThat(captor.getAllValues().get(0), isBsonObject().containing("first_name", 1));
		assertThat(captor.getAllValues().get(1), isBsonObject().containing("$set.last_name", "targaryen"));
	}

	@Test // DATAMONGO-1678
	public void bulkUpdateShouldMapQueryAndUpdateCorrectly() {

		ops.updateMulti(query(where("firstName").is("danerys")), Update.update("firstName", "queen danerys")).execute();

		verify(bulk).find(captor.capture());
		verify(bulkWriteRequestBuilder).update(captor.capture());

		assertThat(captor.getAllValues().get(0), isBsonObject().containing("first_name", "danerys"));
		assertThat(captor.getAllValues().get(1), isBsonObject().containing("$set.first_name", "queen danerys"));
	}

	@Test // DATAMONGO-1678
	public void bulkUpdateManyShouldMapQueryAndUpdateCorrectly() {

		ops.updateOne(query(where("firstName").is("danerys")), Update.update("firstName", "queen danerys")).execute();

		verify(bulk).find(captor.capture());
		verify(bulkWriteRequestBuilder).updateOne(captor.capture());

		assertThat(captor.getAllValues().get(0), isBsonObject().containing("first_name", "danerys"));
		assertThat(captor.getAllValues().get(1), isBsonObject().containing("$set.first_name", "queen danerys"));
	}

	@Test // DATAMONGO-1678
	public void bulkUpsertShouldMapQueryAndUpdateCorrectly() {

		ops.upsert(query(where("firstName").is("danerys")), Update.update("firstName", "queen danerys")).execute();

		verify(bulk).find(captor.capture());
		verify(bulkUpdateRequestBuilder).update(captor.capture());

		assertThat(captor.getAllValues().get(0), isBsonObject().containing("first_name", "danerys"));
		assertThat(captor.getAllValues().get(1), isBsonObject().containing("$set.first_name", "queen danerys"));
	}

	@Test // DATAMONGO-1678
	public void bulkRemoveShouldMapQueryCorrectly() {

		ops.remove(query(where("firstName").is("danerys"))).execute();

		verify(bulk).find(captor.capture());

		assertThat(captor.getValue(), isBsonObject().containing("first_name", "danerys"));
	}

	class SomeDomainType {

		@Id String id;
		Gender gender;
		@Field("first_name") String firstName;
		@Field("last_name") String lastName;
	}

	enum Gender {
		M, F
	}
}
