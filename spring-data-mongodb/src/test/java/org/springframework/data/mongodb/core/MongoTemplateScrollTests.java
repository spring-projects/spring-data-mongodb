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

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Stream;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.data.annotation.PersistenceCreator;
import org.springframework.data.auditing.IsNewAwareAuditingHandler;
import org.springframework.data.domain.KeysetScrollPosition;
import org.springframework.data.domain.OffsetScrollPosition;
import org.springframework.data.domain.ScrollPosition;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Window;
import org.springframework.data.mapping.context.PersistentEntities;
import org.springframework.data.mongodb.core.MongoTemplateTests.PersonWithIdPropertyOfTypeUUIDListener;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.MongoClientExtension;
import org.springframework.data.mongodb.test.util.MongoTestTemplate;

import com.mongodb.client.MongoClient;

/**
 * Integration tests for {@link org.springframework.data.domain.Window} queries.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@ExtendWith(MongoClientExtension.class)
class MongoTemplateScrollTests {

	static @Client MongoClient client;

	public static final String DB_NAME = "mongo-template-scroll-tests";

	ConfigurableApplicationContext context = new GenericApplicationContext();

	MongoTestTemplate template = new MongoTestTemplate(cfg -> {

		cfg.configureDatabaseFactory(it -> {

			it.client(client);
			it.defaultDb(DB_NAME);
		});

		cfg.configureMappingContext(it -> {
			it.autocreateIndex(false);
		});

		cfg.configureApplicationContext(it -> {
			it.applicationContext(context);
			it.addEventListener(new PersonWithIdPropertyOfTypeUUIDListener());
		});

		cfg.configureAuditing(it -> {
			it.auditingHandler(ctx -> {
				return new IsNewAwareAuditingHandler(PersistentEntities.of(ctx));
			});
		});
	});

	@BeforeEach
	void setUp() {
		template.remove(Person.class).all();
		template.remove(WithNestedDocument.class).all();
	}

	@Test // GH-4308
	void shouldUseKeysetScrollingWithNestedSort() {

		WithNestedDocument john20 = new WithNestedDocument(null, "John", 120, new WithNestedDocument("John", 20),
				new Document("name", "bar"));
		WithNestedDocument john40 = new WithNestedDocument(null, "John", 140, new WithNestedDocument("John", 40),
				new Document("name", "baz"));
		WithNestedDocument john41 = new WithNestedDocument(null, "John", 141, new WithNestedDocument("John", 41),
				new Document("name", "foo"));

		template.insertAll(Arrays.asList(john20, john40, john41));

		Query q = new Query(where("name").regex("J.*")).with(Sort.by("nested.name", "nested.age", "document.name"))
				.limit(2);
		q.with(KeysetScrollPosition.initial());

		Window<WithNestedDocument> scroll = template.scroll(q, WithNestedDocument.class);

		assertThat(scroll.hasNext()).isTrue();
		assertThat(scroll.isLast()).isFalse();
		assertThat(scroll).hasSize(2);
		assertThat(scroll).containsOnly(john20, john40);

		scroll = template.scroll(q.with(scroll.positionAt(scroll.size()-1)), WithNestedDocument.class);

		assertThat(scroll.hasNext()).isFalse();
		assertThat(scroll.isLast()).isTrue();
		assertThat(scroll).hasSize(1);
		assertThat(scroll).containsOnly(john41);
	}

	@Test // GH-4308
	void shouldErrorOnNullValueForQuery() {

		WithNestedDocument john20 = new WithNestedDocument(null, "John", 120, new WithNestedDocument("John", 20),
				new Document("name", "bar"));
		WithNestedDocument john40 = new WithNestedDocument(null, "John", 140, new WithNestedDocument("John", 41),
				new Document());
		WithNestedDocument john41 = new WithNestedDocument(null, "John", 140, new WithNestedDocument("John", 41),
				new Document());
		WithNestedDocument john42 = new WithNestedDocument(null, "John", 140, new WithNestedDocument("John", 41),
				new Document());
		WithNestedDocument john43 = new WithNestedDocument(null, "John", 140, new WithNestedDocument("John", 41),
				new Document());
		WithNestedDocument john44 = new WithNestedDocument(null, "John", 141, new WithNestedDocument("John", 41),
				new Document("name", "foo"));

		template.insertAll(Arrays.asList(john20, john40, john41, john42, john43, john44));

		Query q = new Query(where("name").regex("J.*")).with(Sort.by("nested.name", "nested.age", "document.name"))
				.limit(2);
		q.with(KeysetScrollPosition.initial());

		Window<WithNestedDocument> scroll = template.scroll(q, WithNestedDocument.class);

		assertThat(scroll.hasNext()).isTrue();
		assertThat(scroll.isLast()).isFalse();
		assertThat(scroll).hasSize(2);
		assertThat(scroll).containsOnly(john20, john40);

		ScrollPosition startAfter = scroll.positionAt(scroll.size()-1);

		assertThatExceptionOfType(IllegalStateException.class)
				.isThrownBy(() -> template.scroll(q.with(startAfter), WithNestedDocument.class))
				.withMessageContaining("document.name");
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

		template.insertAll(Arrays.asList(john20, john40_1, john40_2, jane_20, jane_40, jane_42));
		Query q = new Query(where("firstName").regex("J.*")).with(Sort.by("firstName", "age")).limit(2);
		q.with(scrollPosition);

		Window<T> scroll = template.scroll(q, resultType, "person");

		assertThat(scroll.hasNext()).isTrue();
		assertThat(scroll.isLast()).isFalse();
		assertThat(scroll).hasSize(2);
		assertThat(scroll).containsOnly(assertionConverter.apply(jane_20), assertionConverter.apply(jane_40));

		scroll = template.scroll(q.with(scroll.positionAt(scroll.size()-1)).limit(3), resultType, "person");

		assertThat(scroll.hasNext()).isTrue();
		assertThat(scroll.isLast()).isFalse();
		assertThat(scroll).hasSize(3);
		assertThat(scroll).contains(assertionConverter.apply(jane_42), assertionConverter.apply(john20));
		assertThat(scroll).containsAnyOf(assertionConverter.apply(john40_1), assertionConverter.apply(john40_2));

		scroll = template.scroll(q.with(scroll.positionAt(scroll.size()-1)).limit(1), resultType, "person");

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

	@NoArgsConstructor
	@Data
	class WithNestedDocument {

		String id;
		String name;

		int age;

		WithNestedDocument nested;

		Document document;

		public WithNestedDocument(String name, int age) {
			this.name = name;
			this.age = age;
		}

		@PersistenceCreator
		public WithNestedDocument(String id, String name, int age, WithNestedDocument nested, Document document) {
			this.id = id;
			this.name = name;
			this.age = age;
			this.nested = nested;
			this.document = document;
		}
	}
}
