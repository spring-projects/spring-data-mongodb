/*
 * Copyright 2017 the original author or authors.
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

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.mongodb.core.mapping.BasicMongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.util.ClassTypeInformation;

/**
 * Unit tests for {@link ObjectPath}.
 * 
 * @author Christoph Strobl
 */
public class ObjectPathUnitTests {

	MongoPersistentEntity<EntityOne> one;
	MongoPersistentEntity<EntityTwo> two;
	MongoPersistentEntity<EntityThree> three;

	@Before
	public void setUp() {

		one = new BasicMongoPersistentEntity(ClassTypeInformation.from(EntityOne.class));
		two = new BasicMongoPersistentEntity(ClassTypeInformation.from(EntityTwo.class));
		three = new BasicMongoPersistentEntity(ClassTypeInformation.from(EntityThree.class));
	}

	@Test // DATAMONGO-1703
	public void getPathItemShouldReturnMatch() {

		ObjectPath path = ObjectPath.ROOT.push(new EntityOne(), one, "id-1");

		assertThat(path.getPathItem("id-1", "one", EntityOne.class), is(notNullValue()));
	}

	@Test // DATAMONGO-1703
	public void getPathItemShouldReturnNullWhenNoTypeMatchFound() {

		ObjectPath path = ObjectPath.ROOT.push(new EntityOne(), one, "id-1");

		assertThat(path.getPathItem("id-1", "one", EntityThree.class), is(nullValue()));
	}

	@Test // DATAMONGO-1703
	public void getPathItemShouldReturnCachedItemWhenIdAndCollectionMatchAndIsAssignable() {

		ObjectPath path = ObjectPath.ROOT.push(new EntityTwo(), one, "id-1");

		assertThat(path.getPathItem("id-1", "one", EntityOne.class), is(notNullValue()));
	}

	@Test // DATAMONGO-1703
	public void getPathItemShouldReturnNullWhenIdAndCollectionMatchButNotAssignable() {

		ObjectPath path = ObjectPath.ROOT.push(new EntityOne(), one, "id-1");

		assertThat(path.getPathItem("id-1", "one", EntityTwo.class), is(nullValue()));
	}

	@Test // DATAMONGO-1703
	public void getPathItemShouldReturnNullWhenIdAndCollectionMatchAndAssignableToInterface() {

		ObjectPath path = ObjectPath.ROOT.push(new EntityThree(), one, "id-1");

		assertThat(path.getPathItem("id-1", "one", ValueInterface.class), is(notNullValue()));
	}

	@Document(collection = "one")
	static class EntityOne {

	}

	static class EntityTwo extends EntityOne {

	}

	interface ValueInterface {

	}

	@Document(collection = "three")
	static class EntityThree implements ValueInterface {

	}
}
