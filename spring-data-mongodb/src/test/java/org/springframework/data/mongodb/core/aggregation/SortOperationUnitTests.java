/*
 * Copyright 2013 the original author or authors.
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
package org.springframework.data.mongodb.core.aggregation;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.springframework.data.mongodb.core.DBObjectUtils.*;

import org.junit.Test;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;

import com.mongodb.DBObject;

/**
 * Unit tests for {@link SortOperation}.
 * 
 * @author Oliver Gierke
 */
public class SortOperationUnitTests {

	@Test
	public void createsDBObjectForAscendingSortCorrectly() {

		SortOperation operation = new SortOperation(new Sort(Direction.ASC, "foobar"));
		DBObject result = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);

		DBObject sortValue = getAsDBObject(result, "$sort");
		assertThat(sortValue, is(notNullValue()));
		assertThat(sortValue.get("foobar"), is((Object) 1));
	}

	@Test
	public void createsDBObjectForDescendingSortCorrectly() {

		SortOperation operation = new SortOperation(new Sort(Direction.DESC, "foobar"));
		DBObject result = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);

		DBObject sortValue = getAsDBObject(result, "$sort");
		assertThat(sortValue, is(notNullValue()));
		assertThat(sortValue.get("foobar"), is((Object) (0 - 1)));
	}
}
