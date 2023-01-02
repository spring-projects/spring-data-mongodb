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
package org.springframework.data.mongodb.core.convert;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.HashSet;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

/**
 * Test case to verify correct usage of custom {@link Converter} implementations to be used.
 *
 * @author Oliver Gierke
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class CustomConvertersUnitTests {

	private MappingMongoConverter converter;

	@Mock BarToDocumentConverter barToDocumentConverter;
	@Mock DocumentToBarConverter documentToBarConverter;

	private MongoMappingContext context;

	@BeforeEach
	void setUp() {

		when(barToDocumentConverter.convert(any(Bar.class))).thenReturn(new Document());
		when(documentToBarConverter.convert(any(Document.class))).thenReturn(new Bar());

		CustomConversions conversions = new MongoCustomConversions(
				Arrays.asList(barToDocumentConverter, documentToBarConverter));

		context = new MongoMappingContext();
		context.setInitialEntitySet(new HashSet<>(Arrays.asList(Foo.class, Bar.class)));
		context.setSimpleTypeHolder(conversions.getSimpleTypeHolder());
		context.initialize();

		converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, context);
		converter.setCustomConversions(conversions);
		converter.afterPropertiesSet();
	}

	@Test // DATADOC-101
	void nestedToDocumentConverterGetsInvoked() {

		Foo foo = new Foo();
		foo.bar = new Bar();

		converter.write(foo, new Document());
		verify(barToDocumentConverter).convert(any(Bar.class));
	}

	@Test // DATADOC-101
	void nestedFromDocumentConverterGetsInvoked() {

		Document document = new Document();
		document.put("bar", new Document());

		converter.read(Foo.class, document);
		verify(documentToBarConverter).convert(any(Document.class));
	}

	@Test // DATADOC-101
	void toDocumentConverterGetsInvoked() {

		converter.write(new Bar(), new Document());
		verify(barToDocumentConverter).convert(any(Bar.class));
	}

	@Test // DATADOC-101
	void fromDocumentConverterGetsInvoked() {

		converter.read(Bar.class, new Document());
		verify(documentToBarConverter).convert(any(Document.class));
	}

	@Test // DATADOC-101
	void foo() {
		Document document = new Document();
		document.put("foo", null);

		assertThat(document).containsKey("foo");
	}

	public static class Foo {
		@Id public String id;
		public Bar bar;
	}

	public static class Bar {
		@Id public String id;
		public String foo;
	}

	private interface BarToDocumentConverter extends Converter<Bar, Document> {}

	private interface DocumentToBarConverter extends Converter<Document, Bar> {}
}
