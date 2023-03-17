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

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Comparator;
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
import org.springframework.data.domain.KeysetScrollPosition.Direction;
import org.springframework.data.domain.OffsetScrollPosition;
import org.springframework.data.domain.ScrollPosition;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Window;
import org.springframework.data.mapping.context.PersistentEntities;
import org.springframework.data.mongodb.core.MongoTemplateTests.PersonWithIdPropertyOfTypeUUIDListener;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.MongoClientExtension;
import org.springframework.data.mongodb.test.util.MongoTestTemplate;
import org.springframework.lang.Nullable;
import org.springframework.util.ObjectUtils;

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

	private static int compareProxies(PersonInterfaceProjection actual, PersonInterfaceProjection expected) {
		if (actual.getAge() != expected.getAge()) {
			return -1;
		}
		if (!ObjectUtils.nullSafeEquals(actual.getFirstName(), expected.getFirstName())) {
			return -1;
		}

		return 0;
	}

	@BeforeEach
	void setUp() {
		template.remove(Person.class).all();
		template.remove(WithNestedDocument.class).all();
		template.remove(WithRenamedField.class).all();
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

		Window<WithNestedDocument> window = template.scroll(q, WithNestedDocument.class);

		assertThat(window.hasNext()).isTrue();
		assertThat(window.isLast()).isFalse();
		assertThat(window).hasSize(2);
		assertThat(window).containsOnly(john20, john40);

		window = template.scroll(q.with(window.positionAt(window.size() - 1)), WithNestedDocument.class);

		assertThat(window.hasNext()).isFalse();
		assertThat(window.isLast()).isTrue();
		assertThat(window).hasSize(1);
		assertThat(window).containsOnly(john41);
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
	}

	@Test // GH-4308
	void shouldAllowReverseSort() {

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

		Window<WithNestedDocument> window = template.scroll(q, WithNestedDocument.class);

		assertThat(window.hasNext()).isTrue();
		assertThat(window.isLast()).isFalse();
		assertThat(window).hasSize(2);
		assertThat(window).containsOnly(john20, john40);

		window = template.scroll(q.with(window.positionAt(window.size() - 1)), WithNestedDocument.class);

		assertThat(window.hasNext()).isFalse();
		assertThat(window.isLast()).isTrue();
		assertThat(window).hasSize(1);
		assertThat(window).containsOnly(john41);

		KeysetScrollPosition scrollPosition = (KeysetScrollPosition) window.positionAt(0);
		KeysetScrollPosition reversePosition = KeysetScrollPosition.of(scrollPosition.getKeys(), Direction.Backward);

		window = template.scroll(q.with(reversePosition), WithNestedDocument.class);

		assertThat(window.hasNext()).isTrue();
		assertThat(window.isLast()).isFalse();
		assertThat(window).hasSize(2);
		assertThat(window).containsOnly(john20, john40);
	}

	@ParameterizedTest // GH-4308
	@MethodSource("positions")
	public <T> void shouldApplyCursoringCorrectly(ScrollPosition scrollPosition, Class<T> resultType,
			Function<Person, T> assertionConverter, @Nullable Comparator<T> comparator) {

		Person john20 = new Person("John", 20);
		Person john40_1 = new Person("John", 40);
		Person john40_2 = new Person("John", 40);
		Person jane_20 = new Person("Jane", 20);
		Person jane_40 = new Person("Jane", 40);
		Person jane_42 = new Person("Jane", 42);

		template.insertAll(Arrays.asList(john20, john40_1, john40_2, jane_20, jane_40, jane_42));
		Query q = new Query(where("firstName").regex("J.*")).with(Sort.by("firstName", "age")).limit(2);

		Window<T> window = template.query(Person.class).inCollection("person").as(resultType).matching(q)
				.scroll(scrollPosition);

		assertThat(window.hasNext()).isTrue();
		assertThat(window.isLast()).isFalse();
		assertThat(window).hasSize(2);
		assertWindow(window, comparator).containsOnly(assertionConverter.apply(jane_20), assertionConverter.apply(jane_40));

		window = template.query(Person.class).inCollection("person").as(resultType).matching(q.limit(3))
				.scroll(window.positionAt(window.size() - 1));

		assertThat(window.hasNext()).isTrue();
		assertThat(window.isLast()).isFalse();
		assertThat(window).hasSize(3);
		assertWindow(window, comparator).contains(assertionConverter.apply(jane_42), assertionConverter.apply(john20));
		assertWindow(window, comparator).containsAnyOf(assertionConverter.apply(john40_1),
				assertionConverter.apply(john40_2));

		window = template.query(Person.class).inCollection("person").as(resultType).matching(q.limit(1))
				.scroll(window.positionAt(window.size() - 1));

		assertThat(window.hasNext()).isFalse();
		assertThat(window.isLast()).isTrue();
		assertThat(window).hasSize(1);
		assertWindow(window, comparator).containsAnyOf(assertionConverter.apply(john40_1),
				assertionConverter.apply(john40_2));
	}

	@ParameterizedTest // GH-4308
	@MethodSource("renamedFieldProjectTargets")
	<T> void scrollThroughResultsWithRenamedField(Class<T> resultType, Function<WithRenamedField, T> assertionConverter) {

		WithRenamedField one = new WithRenamedField("id-1", "v1", null);
		WithRenamedField two = new WithRenamedField("id-2", "v2", null);
		WithRenamedField three = new WithRenamedField("id-3", "v3", null);

		template.insertAll(Arrays.asList(one, two, three));

		Query q = new Query(where("value").regex("v.*")).with(Sort.by(Sort.Direction.DESC, "value")).limit(2);
		q.with(KeysetScrollPosition.initial());

		Window<T> window = template.query(WithRenamedField.class).as(resultType).matching(q)
				.scroll(KeysetScrollPosition.initial());

		assertThat(window.hasNext()).isTrue();
		assertThat(window.isLast()).isFalse();
		assertThat(window).hasSize(2);
		assertThat(window).containsOnly(assertionConverter.apply(three), assertionConverter.apply(two));

		window = template.query(WithRenamedField.class).as(resultType).matching(q)
				.scroll(window.positionAt(window.size() - 1));

		assertThat(window.hasNext()).isFalse();
		assertThat(window.isLast()).isTrue();
		assertThat(window).hasSize(1);
		assertThat(window).containsOnly(assertionConverter.apply(one));
	}

	static Stream<Arguments> positions() {

		return Stream.of(args(KeysetScrollPosition.initial(), Person.class, Function.identity()), //
				args(KeysetScrollPosition.initial(), Document.class, MongoTemplateScrollTests::toDocument), //
				args(OffsetScrollPosition.initial(), Person.class, Function.identity()), //
				args(OffsetScrollPosition.initial(), PersonDtoProjection.class,
						MongoTemplateScrollTests::toPersonDtoProjection), //
				args(OffsetScrollPosition.initial(), PersonInterfaceProjection.class,
						MongoTemplateScrollTests::toPersonInterfaceProjection, MongoTemplateScrollTests::compareProxies));
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

	static <T> org.assertj.core.api.IterableAssert<T> assertWindow(Window<T> window, @Nullable Comparator<T> comparator) {
		return comparator != null ? assertThat(window).usingElementComparator(comparator) : assertThat(window);
	}

	private static <T> Arguments args(ScrollPosition scrollPosition, Class<T> resultType,
			Function<Person, T> assertionConverter) {
		return args(scrollPosition, resultType, assertionConverter, null);
	}

	private static <T> Arguments args(ScrollPosition scrollPosition, Class<T> resultType,
			Function<Person, T> assertionConverter, @Nullable Comparator<T> comparator) {
		return Arguments.of(scrollPosition, resultType, assertionConverter, comparator);
	}

	static Document toDocument(Person person) {

		return new Document("_class", person.getClass().getName()).append("_id", person.getId()).append("active", true)
				.append("firstName", person.getFirstName()).append("age", person.getAge());
	}

	static PersonDtoProjection toPersonDtoProjection(Person person) {

		PersonDtoProjection dto = new PersonDtoProjection();
		dto.firstName = person.getFirstName();
		dto.age = person.getAge();
		return dto;
	}

	static PersonInterfaceProjection toPersonInterfaceProjection(Person person) {

		return new PersonInterfaceProjectionImpl(person);
	}

	@Data
	static class PersonDtoProjection {
		String firstName;
		int age;
	}

	interface PersonInterfaceProjection {
		String getFirstName();

		int getAge();
	}

	static class PersonInterfaceProjectionImpl implements PersonInterfaceProjection {

		final Person delegate;

		public PersonInterfaceProjectionImpl(Person delegate) {
			this.delegate = delegate;
		}

		@Override
		public String getFirstName() {
			return delegate.getFirstName();
		}

		@Override
		public int getAge() {
			return delegate.getAge();
		}

		@Override
		public boolean equals(Object o) {
			if (o instanceof Proxy) {
				return true;
			}
			return false;
		}

		@Override
		public int hashCode() {
			return ObjectUtils.nullSafeHashCode(delegate);
		}
	}

	@Data
	@AllArgsConstructor
	@NoArgsConstructor
	static class WithRenamedField {

		String id;

		@Field("_val") String value;

		WithRenamedField nested;
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
