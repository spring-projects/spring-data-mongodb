/*
 * Copyright 2017-2018 the original author or authors.
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
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import lombok.Data;

import java.util.Optional;

import org.bson.BsonString;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import com.mongodb.MongoClient;
import com.mongodb.client.result.UpdateResult;

/**
 * Integration tests for {@link ExecutableUpdateOperationSupport}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class ExecutableUpdateOperationSupportTests {

	private static final String STAR_WARS = "star-wars";
	MongoTemplate template;

	Person han;
	Person luke;

	@Before
	public void setUp() {

		template = new MongoTemplate(new SimpleMongoDbFactory(new MongoClient(), "ExecutableUpdateOperationSupportTests"));
		template.dropCollection(STAR_WARS);

		han = new Person();
		han.firstname = "han";
		han.id = "id-1";

		luke = new Person();
		luke.firstname = "luke";
		luke.id = "id-2";

		template.save(han);
		template.save(luke);
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1563
	public void domainTypeIsRequired() {
		template.update(null);
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1563
	public void updateIsRequired() {
		template.update(Person.class).apply(null);
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1563
	public void collectionIsRequiredOnSet() {
		template.update(Person.class).inCollection(null);
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1563
	public void findAndModifyOptionsAreRequiredOnSet() {
		template.update(Person.class).apply(new Update()).withOptions(null);
	}

	@Test // DATAMONGO-1563
	public void updateFirst() {

		UpdateResult result = template.update(Person.class).apply(new Update().set("firstname", "Han")).first();

		assertThat(result.getModifiedCount()).isEqualTo(1L);
		assertThat(result.getUpsertedId()).isNull();
	}

	@Test // DATAMONGO-1563
	public void updateAll() {

		UpdateResult result = template.update(Person.class).apply(new Update().set("firstname", "Han")).all();

		assertThat(result.getModifiedCount()).isEqualTo(2L);
		assertThat(result.getUpsertedId()).isNull();
	}

	@Test // DATAMONGO-1563
	public void updateAllMatching() {

		UpdateResult result = template.update(Person.class).matching(queryHan()).apply(new Update().set("firstname", "Han"))
				.all();

		assertThat(result.getModifiedCount()).isEqualTo(1L);
		assertThat(result.getUpsertedId()).isNull();
	}

	@Test // DATAMONGO-1563
	public void updateWithDifferentDomainClassAndCollection() {

		UpdateResult result = template.update(Jedi.class).inCollection(STAR_WARS)
				.matching(query(where("_id").is(han.getId()))).apply(new Update().set("name", "Han")).all();

		assertThat(result.getModifiedCount()).isEqualTo(1L);
		assertThat(result.getUpsertedId()).isNull();
		assertThat(template.findOne(queryHan(), Person.class)).isNotEqualTo(han).hasFieldOrPropertyWithValue("firstname",
				"Han");
	}

	@Test // DATAMONGO-1719
	public void findAndModifyValue() {

		Person result = template.update(Person.class).matching(queryHan()).apply(new Update().set("firstname", "Han"))
				.findAndModifyValue();

		assertThat(result).isEqualTo(han);
		assertThat(template.findOne(queryHan(), Person.class)).isNotEqualTo(han).hasFieldOrPropertyWithValue("firstname",
				"Han");
	}

	@Test // DATAMONGO-1563
	public void findAndModify() {

		Optional<Person> result = template.update(Person.class).matching(queryHan())
				.apply(new Update().set("firstname", "Han")).findAndModify();

		assertThat(result).contains(han);
		assertThat(template.findOne(queryHan(), Person.class)).isNotEqualTo(han).hasFieldOrPropertyWithValue("firstname",
				"Han");
	}

	@Test // DATAMONGO-1563
	public void findAndModifyWithDifferentDomainTypeAndCollection() {

		Optional<Jedi> result = template.update(Jedi.class).inCollection(STAR_WARS)
				.matching(query(where("_id").is(han.getId()))).apply(new Update().set("name", "Han")).findAndModify();

		assertThat(result.get()).hasFieldOrPropertyWithValue("name", "han");
		assertThat(template.findOne(queryHan(), Person.class)).isNotEqualTo(han).hasFieldOrPropertyWithValue("firstname",
				"Han");
	}

	@Test // DATAMONGO-1563
	public void findAndModifyWithOptions() {

		Optional<Person> result = template.update(Person.class).matching(queryHan())
				.apply(new Update().set("firstname", "Han")).withOptions(FindAndModifyOptions.options().returnNew(true))
				.findAndModify();

		assertThat(result.get()).isNotEqualTo(han).hasFieldOrPropertyWithValue("firstname", "Han");
	}

	@Test // DATAMONGO-1563
	public void upsert() {

		UpdateResult result = template.update(Person.class).matching(query(where("id").is("id-3")))
				.apply(new Update().set("firstname", "Chewbacca")).upsert();

		assertThat(result.getModifiedCount()).isEqualTo(0L);
		assertThat(result.getUpsertedId()).isEqualTo(new BsonString("id-3"));
	}

	private Query queryHan() {
		return query(where("id").is(han.getId()));
	}

	@Data
	@org.springframework.data.mongodb.core.mapping.Document(collection = STAR_WARS)
	static class Person {
		@Id String id;
		String firstname;
	}

	@Data
	static class Human {
		@Id String id;
	}

	@Data
	static class Jedi {

		@Field("firstname") String name;
	}
}
