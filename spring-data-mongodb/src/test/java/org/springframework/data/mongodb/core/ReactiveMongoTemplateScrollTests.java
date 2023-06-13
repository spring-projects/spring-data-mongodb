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
import java.util.Objects;
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
import org.springframework.data.domain.ScrollPosition;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Window;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.MongoClientExtension;
import org.springframework.data.mongodb.test.util.ReactiveMongoTestTemplate;

import com.mongodb.reactivestreams.client.MongoClient;

/**
 * Integration tests for {@link Window} queries.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
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

		template.remove(WithRenamedField.class).all() //
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

		Window<T> window = template.scroll(q, resultType, "person").block(Duration.ofSeconds(10));

		assertThat(window.hasNext()).isTrue();
		assertThat(window.isLast()).isFalse();
		assertThat(window).hasSize(2);
		assertThat(window).containsOnly(assertionConverter.apply(jane_20), assertionConverter.apply(jane_40));

		window = template.scroll(q.limit(3).with(window.positionAt(window.size() - 1)), resultType, "person")
				.block(Duration.ofSeconds(10));

		assertThat(window.hasNext()).isTrue();
		assertThat(window.isLast()).isFalse();
		assertThat(window).hasSize(3);
		assertThat(window).contains(assertionConverter.apply(jane_42), assertionConverter.apply(john20));
		assertThat(window).containsAnyOf(assertionConverter.apply(john40_1), assertionConverter.apply(john40_2));

		window = template.scroll(q.limit(1).with(window.positionAt(window.size() - 1)), resultType, "person")
				.block(Duration.ofSeconds(10));

		assertThat(window.hasNext()).isFalse();
		assertThat(window.isLast()).isTrue();
		assertThat(window).hasSize(1);
		assertThat(window).containsAnyOf(assertionConverter.apply(john40_1), assertionConverter.apply(john40_2));
	}

	@ParameterizedTest // GH-4308
	@MethodSource("renamedFieldProjectTargets")
	<T> void scrollThroughResultsWithRenamedField(Class<T> resultType, Function<WithRenamedField, T> assertionConverter) {

		WithRenamedField one = new WithRenamedField("id-1", "v1", null);
		WithRenamedField two = new WithRenamedField("id-2", "v2", null);
		WithRenamedField three = new WithRenamedField("id-3", "v3", null);

		template.insertAll(Arrays.asList(one, two, three)).as(StepVerifier::create).expectNextCount(3).verifyComplete();

		Query q = new Query(where("value").regex("v.*")).with(Sort.by(Sort.Direction.DESC, "value")).limit(2);
		q.with(ScrollPosition.keyset());

		Window<T> window = template.query(WithRenamedField.class).as(resultType).matching(q)
				.scroll(ScrollPosition.keyset()).block(Duration.ofSeconds(10));

		assertThat(window.hasNext()).isTrue();
		assertThat(window.isLast()).isFalse();
		assertThat(window).hasSize(2);
		assertThat(window).containsOnly(assertionConverter.apply(three), assertionConverter.apply(two));

		window = template.query(WithRenamedField.class).as(resultType).matching(q)
				.scroll(window.positionAt(window.size() - 1)).block(Duration.ofSeconds(10));

		assertThat(window.hasNext()).isFalse();
		assertThat(window.isLast()).isTrue();
		assertThat(window).hasSize(1);
		assertThat(window).containsOnly(assertionConverter.apply(one));
	}

	static Stream<Arguments> positions() {

		return Stream.of(args(ScrollPosition.keyset(), Person.class, Function.identity()), //
				args(ScrollPosition.keyset(), Document.class, ReactiveMongoTemplateScrollTests::toDocument), //
				args(ScrollPosition.offset(), Person.class, Function.identity()));
	}

	static Stream<Arguments> renamedFieldProjectTargets() {
		return Stream.of(Arguments.of(WithRenamedField.class, Function.identity()),
				Arguments.of(Document.class, new Function<WithRenamedField, Document>() {
					@Override
					public Document apply(WithRenamedField withRenamedField) {
						return new Document("_id", withRenamedField.getId()).append("_val", withRenamedField.getValue())
								.append("_class", WithRenamedField.class.getName());
					}
				}));
	}

	private static <T> Arguments args(ScrollPosition scrollPosition, Class<T> resultType,
			Function<Person, T> assertionConverter) {
		return Arguments.of(scrollPosition, resultType, assertionConverter);
	}

	static Document toDocument(Person person) {
		return new Document("_class", person.getClass().getName()).append("_id", person.getId()).append("active", true)
				.append("firstName", person.getFirstName()).append("age", person.getAge());
	}

	static class WithRenamedField {

		String id;

		@Field("_val") String value;

		WithRenamedField nested;

		public WithRenamedField() {}

		public WithRenamedField(String id, String value, WithRenamedField nested) {
			this.id = id;
			this.value = value;
			this.nested = nested;
		}

		public String getId() {
			return this.id;
		}

		public String getValue() {
			return this.value;
		}

		public WithRenamedField getNested() {
			return this.nested;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setValue(String value) {
			this.value = value;
		}

		public void setNested(WithRenamedField nested) {
			this.nested = nested;
		}

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			WithRenamedField that = (WithRenamedField) o;
			return Objects.equals(id, that.id) && Objects.equals(value, that.value) && Objects.equals(nested, that.nested);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, value, nested);
		}

		public String toString() {
			return "ReactiveMongoTemplateScrollTests.WithRenamedField(id=" + this.getId() + ", value=" + this.getValue()
					+ ", nested=" + this.getNested() + ")";
		}
	}
}
