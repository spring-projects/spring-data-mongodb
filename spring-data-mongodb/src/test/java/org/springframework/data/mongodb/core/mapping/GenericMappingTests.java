/*
 * Copyright 2011-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.mapping;

import static org.assertj.core.api.Assertions.*;

import java.util.Collections;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;

/**
 * Unit tests for testing the mapping works with generic types.
 *
 * @author Oliver Gierke
 */
@ExtendWith(MockitoExtension.class)
class GenericMappingTests {

	private MongoMappingContext context;
	private MongoConverter converter;

	@Mock DbRefResolver resolver;

	@BeforeEach
	void setUp() throws Exception {

		context = new MongoMappingContext();
		context.setInitialEntitySet(Collections.singleton(StringWrapper.class));
		context.initialize();

		converter = new MappingMongoConverter(resolver, context);
	}

	@Test
	void writesGenericTypeCorrectly() {

		StringWrapper wrapper = new StringWrapper();
		wrapper.container = new Container<>();
		wrapper.container.content = "Foo";

		Document document = new Document();
		converter.write(wrapper, document);

		Object container = document.get("container");
		assertThat(container).isNotNull();
		assertThat(container instanceof Document).isTrue();

		Object content = ((Document) container).get("content");
		assertThat(content instanceof String).isTrue();
		assertThat((String) content).isEqualTo("Foo");
	}

	@Test
	void readsGenericTypeCorrectly() {

		Document content = new Document("content", "Foo");
		Document container = new Document("container", content);

		StringWrapper result = converter.read(StringWrapper.class, container);
		assertThat(result.container).isNotNull();
		assertThat(result.container.content).isEqualTo("Foo");
	}

	private static class StringWrapper extends Wrapper<String> {

	}

	static class Wrapper<S> {
		Container<S> container;
	}

	static class Container<T> {
		T content;
	}
}
