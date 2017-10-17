/*
 * Copyright 2012-2017 the original author or authors.
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
package org.springframework.data.mongodb.core.convert;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import lombok.Data;

import java.util.UUID;

import org.bson.types.Binary;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Integration tests for {@link MongoConverters}.
 * 
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:infrastructure.xml")
public class MongoConvertersIntegrationTests {

	static final String COLLECTION = "converter-tests";

	@Autowired MongoOperations template;

	@Before
	public void setUp() {
		template.dropCollection(COLLECTION);
	}

	@Test // DATAMONGO-422
	public void writesUUIDBinaryCorrectly() {

		Wrapper wrapper = new Wrapper();
		wrapper.uuid = UUID.randomUUID();
		template.save(wrapper);

		assertThat(wrapper.id).isNotNull();

		Wrapper result = template.findOne(Query.query(Criteria.where("id").is(wrapper.id)), Wrapper.class);
		assertThat(result.uuid).isEqualTo(wrapper.uuid);
	}

	@Test // DATAMONGO-1802
	public void shouldConvertBinaryDataOnRead() {

		WithBinaryDataInArray wbd = new WithBinaryDataInArray();
		wbd.data = "calliope-mini".getBytes();

		template.save(wbd);

		assertThat(template.findOne(query(where("id").is(wbd.id)), WithBinaryDataInArray.class)).isEqualTo(wbd);
	}

	@Test // DATAMONGO-1802
	public void shouldConvertEmptyBinaryDataOnRead() {

		WithBinaryDataInArray wbd = new WithBinaryDataInArray();
		wbd.data = new byte[] {};

		template.save(wbd);

		assertThat(template.findOne(query(where("id").is(wbd.id)), WithBinaryDataInArray.class)).isEqualTo(wbd);
	}

	@Test // DATAMONGO-1802
	public void shouldReadBinaryType() {

		WithBinaryDataType wbd = new WithBinaryDataType();
		wbd.data = new Binary("calliope-mini".getBytes());

		template.save(wbd);

		assertThat(template.findOne(query(where("id").is(wbd.id)), WithBinaryDataType.class)).isEqualTo(wbd);
	}

	@Document(collection = COLLECTION)
	static class Wrapper {

		String id;
		UUID uuid;
	}

	@Data
	@Document(collection = COLLECTION)
	static class WithBinaryDataInArray {

		@Id String id;
		byte[] data;
	}

	@Data
	@Document(collection = COLLECTION)
	static class WithBinaryDataType {

		@Id String id;
		Binary data;
	}
}
