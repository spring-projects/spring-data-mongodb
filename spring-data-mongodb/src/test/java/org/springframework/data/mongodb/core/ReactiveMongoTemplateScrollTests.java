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
package org.springframework.data.mongodb.core;

import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.test.util.Assertions.*;

import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Stream;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.data.domain.KeysetScrollPosition;
import org.springframework.data.domain.OffsetScrollPosition;
import org.springframework.data.domain.Window;
import org.springframework.data.domain.ScrollPosition;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.MongoClientExtension;
import org.springframework.data.mongodb.test.util.ReactiveMongoTestTemplate;

import com.mongodb.reactivestreams.client.MongoClient;

/**
 * Integration tests for {@link Window} queries.
 *
 * @author Mark Paluch
 */
@ExtendWith(MongoClientExtension.class)
class ReactiveMongoTemplateScrollTests {

	static @Client MongoClient client;

	public static final String DB_NAME = "mongo-template-scroll-tests";

	ConfigurableApplicationContext context = new GenericApplicationContext();

	private ReactiveMongoTestTemplate template = new ReactiveMongoTestTemplate(cfg -> {

		cfg.configureDatabaseFactory(it -> {

			it.client(client);
			it.defaultDb(DB_NAME);
		});

		cfg.configureApplicationContext(it -> {
			it.applicationContext(context);
		});
	});

	@BeforeEach
	void setUp() {
		template.remove(Person.class).all() //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();
	}

	@ParameterizedTest // GH-4308
	@MethodSource("positions")
	public <T> void shouldApplyCursoringCorrectly(ScrollPosition scrollPosition, Class<T> resultType,
			Function<Person, T> assertionConverter) {

		Person john20 = new Person("John", 20);
		Person john40_1 = new Person("John", 40);
		Person john40_2 = new Person("John", 40);
		Person jane_20 = new Person("Jane", 20);
		Person jane_40 = new Person("Jane", 40);
		Person jane_42 = new Person("Jane", 42);

		template.insertAll(Arrays.asList(john20, john40_1, john40_2, jane_20, jane_40, jane_42)) //
				.as(StepVerifier::create) //
				.expectNextCount(6) //
				.verifyComplete();

		Query q = new Query(where("firstName").regex("J.*")).with(Sort.by("firstName", "age")).limit(2);
		q.with(scrollPosition);

		Window<T> scroll = template.scroll(q, resultType, "person").block(Duration.ofSeconds(10));

		assertThat(scroll.hasNext()).isTrue();
		assertThat(scroll.isLast()).isFalse();
		assertThat(scroll).hasSize(2);
		assertThat(scroll).containsOnly(assertionConverter.apply(jane_20), assertionConverter.apply(jane_40));

		scroll = template.scroll(q.limit(3).with(scroll.positionAt(scroll.size() - 1)), resultType, "person")
				.block(Duration.ofSeconds(10));

		assertThat(scroll.hasNext()).isTrue();
		assertThat(scroll.isLast()).isFalse();
		assertThat(scroll).hasSize(3);
		assertThat(scroll).contains(assertionConverter.apply(jane_42), assertionConverter.apply(john20));
		assertThat(scroll).containsAnyOf(assertionConverter.apply(john40_1), assertionConverter.apply(john40_2));

		scroll = template.scroll(q.limit(1).with(scroll.positionAt(scroll.size() - 1)), resultType, "person")
				.block(Duration.ofSeconds(10));

		assertThat(scroll.hasNext()).isFalse();
		assertThat(scroll.isLast()).isTrue();
		assertThat(scroll).hasSize(1);
		assertThat(scroll).containsAnyOf(assertionConverter.apply(john40_1), assertionConverter.apply(john40_2));
	}

	static Stream<Arguments> positions() {

		return Stream.of(args(KeysetScrollPosition.initial(), Person.class, Function.identity()), //
				args(KeysetScrollPosition.initial(), Document.class, MongoTemplateScrollTests::toDocument), //
				args(OffsetScrollPosition.initial(), Person.class, Function.identity()));
	}

	private static <T> Arguments args(ScrollPosition scrollPosition, Class<T> resultType,
			Function<Person, T> assertionConverter) {
		return Arguments.of(scrollPosition, resultType, assertionConverter);
	}

	static Document toDocument(Person person) {
		return new Document("_class", person.getClass().getName()).append("_id", person.getId()).append("active", true)
				.append("firstName", person.getFirstName()).append("age", person.getAge());
	}
}
