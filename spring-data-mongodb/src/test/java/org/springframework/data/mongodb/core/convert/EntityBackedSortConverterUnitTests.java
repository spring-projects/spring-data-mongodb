/*
 * Copyright 2014 the original author or authors.
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

import static org.hamcrest.core.IsNull.*;
import static org.junit.Assert.*;

import org.hamcrest.core.IsEqual;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;

/**
 * @author Christoph Strobl
 */
public class EntityBackedSortConverterUnitTests {

	private EntityBackedSortConverter converter;

	@Before
	public void setUp() {
		MongoMappingContext context = new MongoMappingContext();
		this.converter = new EntityBackedSortConverter(context.getPersistentEntity(DomainObject.class));
	}

	/**
	 * @see DATAMONGO-888
	 */
	@Test
	public void converterShouldReturnNullOnGivenNullSort() {
		assertThat(converter.convert(null), nullValue());
	}

	/**
	 * @see DATAMONGO-888
	 */
	@Test
	public void converterShouldConvertIdPropertyCorrectly() {

		Sort sort = new Sort(Direction.DESC, "idProperty");
		assertThat(converter.convert(sort), IsEqual.<DBObject> equalTo(new BasicDBObject("_id", -1)));
	}

	/**
	 * @see DATAMONGO-888
	 */
	@Test
	public void converterShouldNotConvertNativeProperties() {

		Sort sort = new Sort(Direction.DESC, "_id");
		assertThat(converter.convert(sort), IsEqual.<DBObject> equalTo(new BasicDBObject("_id", -1)));
	}

	/**
	 * @see DATAMONGO-888
	 */
	@Test
	public void converterShouldNotRemovePropertiesThatDoNotExistInEntity() {

		Sort sort = new Sort(Direction.DESC, "foo");
		assertThat(converter.convert(sort), IsEqual.<DBObject> equalTo(new BasicDBObject("foo", -1)));
	}

	/**
	 * @see DATAMONGO-888
	 */
	@Test
	public void converterShouldConvertFieldAnnotatedPropertyCorrectly() {

		Sort sort = new Sort(Direction.DESC, "mappedProperty");
		assertThat(converter.convert(sort), IsEqual.<DBObject> equalTo(new BasicDBObject("mapped", -1)));
	}

	/**
	 * @see DATAMONGO-888
	 */
	@Test
	public void converterShouldConvertPlainPropertyCorrectly() {

		Sort sort = new Sort(Direction.DESC, "plainProperty");
		assertThat(converter.convert(sort), IsEqual.<DBObject> equalTo(new BasicDBObject("plainProperty", -1)));
	}

	/**
	 * @see DATAMONGO-888
	 */
	@Test
	public void converterShouldNotTryToLookUpPropertiesInCaseBackingEntityIsNull() {

		Sort sort = new Sort(Direction.DESC, "mappedProperty");
		assertThat(new EntityBackedSortConverter(null).convert(sort),
				IsEqual.<DBObject> equalTo(new BasicDBObject("mappedProperty", -1)));
	}

	/**
	 * @see DATAMONGO-888
	 */
	@Test
	public void converterShoudRetainPropertyOrderCorrectly() {

		Sort sort = new Sort(Direction.DESC, "plainProperty", "idProperty").and(new Sort(Direction.ASC, "mappedProperty",
				"foo"));

		assertThat(
				converter.convert(sort),
				IsEqual.<DBObject> equalTo(BasicDBObjectBuilder.start("plainProperty", -1).add("_id", -1).add("mapped", 1)
						.add("foo", 1).get()));
	}

	static class DomainObject {

		@Id String idProperty;

		@Field("mapped") String mappedProperty;

		String plainProperty;

	}
}
