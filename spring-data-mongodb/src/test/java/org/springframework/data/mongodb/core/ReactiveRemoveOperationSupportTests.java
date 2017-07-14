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
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import lombok.Data;
import reactor.test.StepVerifier;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Field;

import com.mongodb.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;

/**
 * Integration tests for {@link ReactiveRemoveOperationSupport}.
 *
 * @author Mark Paluch
 */
public class ReactiveRemoveOperationSupportTests {

	private static final String STAR_WARS = "star-wars";
	MongoTemplate blocking;
	ReactiveMongoTemplate template;

	Person han;
	Person luke;

	@Before
	public void setUp() {

		blocking = new MongoTemplate(new SimpleMongoDbFactory(new MongoClient(), "ExecutableRemoveOperationSupportTests"));
		blocking.dropCollection(STAR_WARS);

		han = new Person();
		han.firstname = "han";
		han.id = "id-1";

		luke = new Person();
		luke.firstname = "luke";
		luke.id = "id-2";

		blocking.save(han);
		blocking.save(luke);

		template = new ReactiveMongoTemplate(MongoClients.create(), "ExecutableRemoveOperationSupportTests");
	}

	@Test // DATAMONGO-1719
	public void removeAll() {

		StepVerifier.create(template.remove(Person.class).all()).consumeNextWith(actual -> {
			assertThat(actual.getDeletedCount()).isEqualTo(2L);
		}).verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void removeAllMatching() {

		StepVerifier.create(template.remove(Person.class).matching(query(where("firstname").is("han"))).all())
				.consumeNextWith(actual -> assertThat(actual.getDeletedCount()).isEqualTo(1L)).verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void removeAllMatchingWithAlternateDomainTypeAndCollection() {

		StepVerifier
				.create(template.remove(Jedi.class).inCollection(STAR_WARS).matching(query(where("name").is("luke"))).all())
				.consumeNextWith(actual -> assertThat(actual.getDeletedCount()).isEqualTo(1L)).verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void removeAndReturnAllMatching() {

		StepVerifier.create(template.remove(Person.class).matching(query(where("firstname").is("han"))).findAndRemove())
				.expectNext(han).verifyComplete();
	}

	@Data
	@org.springframework.data.mongodb.core.mapping.Document(collection = STAR_WARS)
	static class Person {
		@Id String id;
		String firstname;
	}

	@Data
	static class Jedi {

		@Field("firstname") String name;
	}
}
