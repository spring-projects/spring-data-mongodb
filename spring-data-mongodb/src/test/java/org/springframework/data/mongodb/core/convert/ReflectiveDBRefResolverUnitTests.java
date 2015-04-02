/*
 * Copyright 2015 the original author or authors.
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
package org.springframework.data.mongodb.core.convert;

import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsNull.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.mongodb.util.MongoClientVersion.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.mongodb.MongoDbFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBRef;

/**
 * Unit tests for {@link ReflectiveDBRefResolver}.
 * 
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class ReflectiveDBRefResolverUnitTests {

	@Mock MongoDbFactory dbFactoryMock;
	@Mock DBRef dbRefMock;
	@Mock DB dbMock;
	@Mock DBCollection collectionMock;

	@Before
	public void setUp() {

		when(dbRefMock.getCollectionName()).thenReturn("collection-1");
		when(dbRefMock.getId()).thenReturn("id-1");
		when(dbFactoryMock.getDb()).thenReturn(dbMock);
		when(dbMock.getCollection(eq("collection-1"))).thenReturn(collectionMock);
		when(collectionMock.findOne(eq("id-1"))).thenReturn(new BasicDBObject("_id", "id-1"));
	}

	/**
	 * @see DATAMONGO-1193
	 */
	@Test
	public void fetchShouldNotLookUpDbWhenUsingDriverVersion2() {

		assumeThat(isMongo3Driver(), is(false));

		ReflectiveDBRefResolver.fetch(dbFactoryMock, dbRefMock);

		verify(dbFactoryMock, never()).getDb();
		verify(dbFactoryMock, never()).getDb(anyString());
	}

	/**
	 * @see DATAMONGO-1193
	 */
	@Test
	public void fetchShouldUseDbToResolveDbRefWhenUsingDriverVersion3() {

		assumeThat(isMongo3Driver(), is(true));

		assertThat(ReflectiveDBRefResolver.fetch(dbFactoryMock, dbRefMock), notNullValue());
		verify(dbFactoryMock, times(1)).getDb();
	}

	/**
	 * @see DATAMONGO-1193
	 */
	@Test(expected = IllegalArgumentException.class)
	public void fetchShouldThrowExceptionWhenDbFactoryIsNullUsingDriverVersion3() {

		assumeThat(isMongo3Driver(), is(true));

		ReflectiveDBRefResolver.fetch(null, dbRefMock);
	}
}
