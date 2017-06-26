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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.any;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import java.util.List;

import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
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

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.DeleteManyModel;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;

/**
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultBulkOperationsUnitTests {

	@Mock MongoTemplate template;
	@Mock MongoCollection<Document> collection;
	@Mock DbRefResolver dbRefResolver;
	@Captor ArgumentCaptor<List<WriteModel<Document>>> captor;
	MongoConverter converter;
	MongoMappingContext mappingContext;

	DefaultBulkOperations ops;

	@Before
	public void setUp() {

		mappingContext = new MongoMappingContext();
		mappingContext.afterPropertiesSet();

		converter = new MappingMongoConverter(dbRefResolver, mappingContext);

		when(template.getCollection(anyString())).thenReturn(collection);

		ops = new DefaultBulkOperations(template, "collection-1",
				new BulkOperationContext(BulkMode.ORDERED, mappingContext.getPersistentEntity(SomeDomainType.class),
						new QueryMapper(converter), new UpdateMapper(converter)));
	}

	@Test // DATAMONGO-1518
	public void updateOneShouldUseCollationWhenPresent() {

		ops.updateOne(new BasicQuery("{}").collation(Collation.of("de")), new Update().set("lastName", "targaryen"))
				.execute();

		verify(collection).bulkWrite(captor.capture(), any());

		assertThat(captor.getValue().get(0)).isInstanceOf(UpdateOneModel.class);
		assertThat(((UpdateOneModel<Document>) captor.getValue().get(0)).getOptions().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("de").build());
	}

	@Test // DATAMONGO-1518
	public void updateMayShouldUseCollationWhenPresent() {

		ops.updateMulti(new BasicQuery("{}").collation(Collation.of("de")), new Update().set("lastName", "targaryen"))
				.execute();

		verify(collection).bulkWrite(captor.capture(), any());

		assertThat(captor.getValue().get(0)).isInstanceOf(UpdateManyModel.class);
		assertThat(((UpdateManyModel<Document>) captor.getValue().get(0)).getOptions().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("de").build());
	}

	@Test // DATAMONGO-1518
	public void removeShouldUseCollationWhenPresent() {

		ops.remove(new BasicQuery("{}").collation(Collation.of("de"))).execute();

		verify(collection).bulkWrite(captor.capture(), any());

		assertThat(captor.getValue().get(0)).isInstanceOf(DeleteManyModel.class);
		assertThat(((DeleteManyModel<Document>) captor.getValue().get(0)).getOptions().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("de").build());
	}

	@Test // DATAMONGO-1678
	public void bulkUpdateShouldMapQueryAndUpdateCorrectly() {

		ops.updateOne(query(where("firstName").is("danerys")), Update.update("firstName", "queen danerys")).execute();

		verify(collection).bulkWrite(captor.capture(), any());

		UpdateOneModel<Document> updateModel = (UpdateOneModel<Document>) captor.getValue().get(0);
		assertThat(updateModel.getFilter()).isEqualTo(new Document("first_name", "danerys"));
		assertThat(updateModel.getUpdate()).isEqualTo(new Document("$set", new Document("first_name", "queen danerys")));
	}

	@Test // DATAMONGO-1678
	public void bulkRemoveShouldMapQueryCorrectly() {

		ops.remove(query(where("firstName").is("danerys"))).execute();

		verify(collection).bulkWrite(captor.capture(), any());

		DeleteManyModel<Document> updateModel = (DeleteManyModel<Document>) captor.getValue().get(0);
		assertThat(updateModel.getFilter()).isEqualTo(new Document("first_name", "danerys"));
	}

	class SomeDomainType {

		@Id String id;
		Gender gender;
		@Field("first_name") String firstName;
		@Field String lastName;
	}

	enum Gender {
		M, F
	}
}
