/*
 * Copyright (c) 2011 by the original author(s).
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

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.HashSet;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Test case to verify correct usage of custom {@link Converter} implementations to be used.
 * 
 * @author Oliver Gierke
 * @see DATADOC-101
 */
@RunWith(MockitoJUnitRunner.class)
public class CustomConvertersUnitTests {

	MappingMongoConverter converter;

	@Mock BarToDBObjectConverter barToDBObjectConverter;
	@Mock DBObjectToBarConverter dbObjectToBarConverter;
	@Mock MongoDbFactory mongoDbFactory;

	MongoMappingContext context;
	MongoPersistentEntity<Foo> fooEntity;
	MongoPersistentEntity<Bar> barEntity;

	@Before
	@SuppressWarnings("unchecked")
	public void setUp() throws Exception {

		when(barToDBObjectConverter.convert(any(Bar.class))).thenReturn(new BasicDBObject());
		when(dbObjectToBarConverter.convert(any(DBObject.class))).thenReturn(new Bar());

		CustomConversions conversions = new CustomConversions(Arrays.asList(barToDBObjectConverter, dbObjectToBarConverter));

		context = new MongoMappingContext();
		context.setInitialEntitySet(new HashSet<Class<?>>(Arrays.asList(Foo.class, Bar.class)));
		context.setSimpleTypeHolder(conversions.getSimpleTypeHolder());
		context.initialize();

		converter = new MappingMongoConverter(new DefaultDbRefResolver(mongoDbFactory), context);
		converter.setCustomConversions(conversions);
		converter.afterPropertiesSet();
	}

	@Test
	public void nestedToDBObjectConverterGetsInvoked() {

		Foo foo = new Foo();
		foo.bar = new Bar();

		converter.write(foo, new BasicDBObject());
		verify(barToDBObjectConverter).convert(any(Bar.class));
	}

	@Test
	public void nestedFromDBObjectConverterGetsInvoked() {

		BasicDBObject dbObject = new BasicDBObject();
		dbObject.put("bar", new BasicDBObject());

		converter.read(Foo.class, dbObject);
		verify(dbObjectToBarConverter).convert(any(DBObject.class));
	}

	@Test
	public void toDBObjectConverterGetsInvoked() {

		converter.write(new Bar(), new BasicDBObject());
		verify(barToDBObjectConverter).convert(any(Bar.class));
	}

	@Test
	public void fromDBObjectConverterGetsInvoked() {

		converter.read(Bar.class, new BasicDBObject());
		verify(dbObjectToBarConverter).convert(any(DBObject.class));
	}

	@Test
	public void foo() {
		DBObject dbObject = new BasicDBObject();
		dbObject.put("foo", null);

		Assert.assertThat(dbObject.containsField("foo"), CoreMatchers.is(true));
	}

	public static class Foo {
		@Id public String id;
		public Bar bar;
	}

	public static class Bar {
		@Id public String id;
		public String foo;
	}

	private interface BarToDBObjectConverter extends Converter<Bar, DBObject> {

	}

	private interface DBObjectToBarConverter extends Converter<DBObject, Bar> {

	}
}
