/*
 * Copyright 2011-2013 by the original author(s).
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
package org.springframework.data.mongodb.core.mapping;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.lang.reflect.Field;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.context.AbstractMappingContext;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.util.ReflectionUtils;

import com.mongodb.DBRef;

/**
 * Unit tests for {@link MongoMappingContext}.
 * 
 * @author Oliver Gierke
 */
@RunWith(MockitoJUnitRunner.class)
public class MongoMappingContextUnitTests {

	@Mock
	ApplicationContext applicationContext;

	@Test
	public void addsSelfReferencingPersistentEntityCorrectly() throws Exception {

		MongoMappingContext context = new MongoMappingContext();

		context.setInitialEntitySet(Collections.singleton(SampleClass.class));
		context.initialize();
	}

	@Test(expected = MappingException.class)
	public void rejectsEntityWithMultipleIdProperties() {

		MongoMappingContext context = new MongoMappingContext();
		context.getPersistentEntity(ClassWithMultipleIdProperties.class);
	}

	@Test
	public void doesNotReturnPersistentEntityForMongoSimpleType() {

		MongoMappingContext context = new MongoMappingContext();
		assertThat(context.getPersistentEntity(DBRef.class), is(nullValue()));
	}

	@Test
	public void populatesAbstractMappingContextsApplicationCorrectly() {

		MongoMappingContext context = new MongoMappingContext();
		context.setApplicationContext(applicationContext);

		Field field = ReflectionUtils.findField(AbstractMappingContext.class, "applicationContext");
		ReflectionUtils.makeAccessible(field);
		assertThat(ReflectionUtils.getField(field, context), is(notNullValue()));
	}

	/**
	 * @see DATAMONGO-638
	 */
	@Test
	public void doesNotCreatePersistentEntityForAbstractMap() {

		MongoMappingContext context = new MongoMappingContext();
		assertThat(context.getPersistentEntity(AbstractMap.class), is(nullValue()));
	}

	class ClassWithMultipleIdProperties {

		@Id
		String myId;

		String id;
	}

	public class SampleClass {

		Map<String, SampleClass> children;
	}
}
