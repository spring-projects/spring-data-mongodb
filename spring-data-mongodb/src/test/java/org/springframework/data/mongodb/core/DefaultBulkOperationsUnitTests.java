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

import java.util.List;

import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.BulkOperations.BulkMode;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.mapping.Field;
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
	@Mock MongoCollection collection;

	DefaultBulkOperations ops;

	@Before
	public void setUp() {

		when(template.getCollection(anyString())).thenReturn(collection);
		ops = new DefaultBulkOperations(template, BulkMode.ORDERED, "collection-1", SomeDomainType.class);
	}

	@Test // DATAMONGO-1518
	public void updateOneShouldUseCollationWhenPresent() {

		ops.updateOne(new BasicQuery("{}").collation(Collation.of("de")), new Update().set("lastName", "targaryen"))
				.execute();

		ArgumentCaptor<List<WriteModel<Document>>> captor = ArgumentCaptor.forClass(List.class);

		verify(collection).bulkWrite(captor.capture(), any());

		assertThat(captor.getValue().get(0)).isInstanceOf(UpdateOneModel.class);
		assertThat(((UpdateOneModel) captor.getValue().get(0)).getOptions().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("de").build());
	}

	@Test // DATAMONGO-1518
	public void updateMayShouldUseCollationWhenPresent() {

		ops.updateMulti(new BasicQuery("{}").collation(Collation.of("de")), new Update().set("lastName", "targaryen"))
				.execute();

		ArgumentCaptor<List<WriteModel<Document>>> captor = ArgumentCaptor.forClass(List.class);

		verify(collection).bulkWrite(captor.capture(), any());

		assertThat(captor.getValue().get(0)).isInstanceOf(UpdateManyModel.class);
		assertThat(((UpdateManyModel) captor.getValue().get(0)).getOptions().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("de").build());
	}

	@Test // DATAMONGO-1518
	public void removeShouldUseCollationWhenPresent() {

		ops.remove(new BasicQuery("{}").collation(Collation.of("de"))).execute();

		ArgumentCaptor<List<WriteModel<Document>>> captor = ArgumentCaptor.forClass(List.class);

		verify(collection).bulkWrite(captor.capture(), any());

		assertThat(captor.getValue().get(0)).isInstanceOf(DeleteManyModel.class);
		assertThat(((DeleteManyModel) captor.getValue().get(0)).getOptions().getCollation())
				.isEqualTo(com.mongodb.client.model.Collation.builder().locale("de").build());
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
