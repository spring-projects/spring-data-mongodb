/*
 * Copyright (c) 2011-2016 by the original author(s).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.convert;

import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.HashSet;

import org.bson.Document;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;

/**
 * Test case to verify correct usage of custom {@link Converter} implementations to be used.
 * 
 * @author Oliver Gierke
 */
@RunWith(MockitoJUnitRunner.class)
public class CustomConvertersUnitTests {

	MappingMongoConverter converter;

	@Mock BarToDocumentConverter barToDocumentConverter;
	@Mock DocumentToBarConverter documentToBarConverter;
	@Mock MongoDbFactory mongoDbFactory;

	MongoMappingContext context;
	MongoPersistentEntity<Foo> fooEntity;
	MongoPersistentEntity<Bar> barEntity;

	@Before
	@SuppressWarnings("unchecked")
	public void setUp() throws Exception {

		when(barToDocumentConverter.convert(any(Bar.class))).thenReturn(new Document());
		when(documentToBarConverter.convert(any(Document.class))).thenReturn(new Bar());

		CustomConversions conversions = new CustomConversions(
				Arrays.asList(barToDocumentConverter, documentToBarConverter));

		context = new MongoMappingContext();
		context.setInitialEntitySet(new HashSet<Class<?>>(Arrays.asList(Foo.class, Bar.class)));
		context.setSimpleTypeHolder(conversions.getSimpleTypeHolder());
		context.initialize();

		converter = new MappingMongoConverter(new DefaultDbRefResolver(mongoDbFactory), context);
		converter.setCustomConversions(conversions);
		converter.afterPropertiesSet();
	}

	@Test // DATADOC-101
	public void nestedToDocumentConverterGetsInvoked() {

		Foo foo = new Foo();
		foo.bar = new Bar();

		converter.write(foo, new Document());
		verify(barToDocumentConverter).convert(any(Bar.class));
	}

	@Test // DATADOC-101
	public void nestedFromDocumentConverterGetsInvoked() {

		Document document = new Document();
		document.put("bar", new Document());

		converter.read(Foo.class, document);
		verify(documentToBarConverter).convert(any(Document.class));
	}

	@Test // DATADOC-101
	public void toDocumentConverterGetsInvoked() {

		converter.write(new Bar(), new Document());
		verify(barToDocumentConverter).convert(any(Bar.class));
	}

	@Test // DATADOC-101
	public void fromDocumentConverterGetsInvoked() {

		converter.read(Bar.class, new Document());
		verify(documentToBarConverter).convert(any(Document.class));
	}

	@Test // DATADOC-101
	public void foo() {
		Document document = new Document();
		document.put("foo", null);

		Assert.assertThat(document.containsKey("foo"), CoreMatchers.is(true));
	}

	public static class Foo {
		@Id public String id;
		public Bar bar;
	}

	public static class Bar {
		@Id public String id;
		public String foo;
	}

	private interface BarToDocumentConverter extends Converter<Bar, Document> {

	}

	private interface DocumentToBarConverter extends Converter<Document, Bar> {

	}
}
