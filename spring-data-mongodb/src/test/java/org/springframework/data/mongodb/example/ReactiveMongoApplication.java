/*
 * Copyright 2023 the original author or authors.
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
// tag::file[]
package org.springframework.data.mongodb.example;

import static org.springframework.data.mongodb.core.query.Criteria.*;

import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;

import com.mongodb.reactivestreams.client.MongoClients;

public class ReactiveMongoApplication {

	public static void main(String[] args) throws Exception {

		ReactiveMongoOperations mongoOps = new ReactiveMongoTemplate(MongoClients.create(), "database");

		mongoOps.insert(new Person("Joe", 34))
			.then(mongoOps.query(Person.class).matching(where("name").is("Joe")).first())
			.doOnNext(System.out::println)
			.block();

		mongoOps.dropCollection("person").block();
	}
}
// end::file[]
