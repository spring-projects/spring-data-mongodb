/*
 * Copyright 2011 the original author or authors.
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
package org.springframework.data.mongodb.repository.query;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.util.Arrays;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

import com.mongodb.BasicDBList;

/**
 * Unit tests for {@link ConvertingParameterAccessor}.
 *
 * @author Oliver Gierke
 */
@RunWith(MockitoJUnitRunner.class)
public class ConvertingParameterAccessorUnitTests {

	@Mock
	MongoDbFactory factory;
	@Mock
	MongoParameterAccessor accessor;
	
	MongoMappingContext context;
	MappingMongoConverter converter;
	
	@Before
	public void setUp() {
		context = new MongoMappingContext();
		converter = new MappingMongoConverter(factory, context);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullWriter() {
		new MappingMongoConverter(null, context);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullContext() {
		new MappingMongoConverter(factory, null);
	}
	
	@Test
	public void convertsCollectionUponAccess() {
		
		when(accessor.getBindableValue(0)).thenReturn(Arrays.asList("Foo"));
		
		ConvertingParameterAccessor parameterAccessor = new ConvertingParameterAccessor(converter, accessor);
		Object result = parameterAccessor.getBindableValue(0);
		
		BasicDBList reference = new BasicDBList();
		reference.add("Foo");
		
		assertThat(result, is((Object) reference));
	}
}
