/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.mongodb.core.messaging;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;

import org.bson.Document;
import org.junit.Test;
import org.springframework.data.mongodb.core.messaging.DefaultMessageListenerContainerTests.Person;
import org.springframework.data.mongodb.core.query.Query;

/**
 * Unit tests for {@link TailableCursorRequest}.
 *
 * @author Mark Paluch
 */
public class TailableCursorRequestUnitTests {

	@Test // DATAMONGO-1803
	public void shouldBuildRequest() {

		MessageListener<Document, Person> listener = System.out::println;

		TailableCursorRequest<Person> request = TailableCursorRequest.builder(listener).collection("foo")
				.filter(Query.query(where("firstname").is("bar"))).build();

		assertThat(request.getRequestOptions().getCollectionName()).isEqualTo("foo");
		assertThat(request.getRequestOptions().getQuery()).isPresent();
		assertThat(request.getMessageListener()).isEqualTo(listener);
	}
}
