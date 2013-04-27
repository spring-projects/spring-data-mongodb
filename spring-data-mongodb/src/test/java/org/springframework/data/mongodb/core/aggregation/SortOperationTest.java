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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.springframework.data.mongodb.core.aggregation.SortOperation.sort;
import static org.springframework.data.mongodb.core.aggregation.SortOperation.SortOrder.ASCENDING;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.mongodb.DBObject;

/**
 * Tests of {@link SortOperation}.
 * 
 * @see DATAMONGO-586
 * @author Sebastian Herold
 */
public class SortOperationTest {

	@Test
	public void sortMultipleFields() throws Exception {
		SortOperation sortOperation = sort().field("field", ASCENDING).desc("field2").asc("field3");
		assertField(sortOperation, 0, "field", 1);
		assertField(sortOperation, 1, "field2", -1);
		assertField(sortOperation, 2, "field3", 1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void orderNotNull() throws Exception {
		sort().field("field", null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void fieldNameNotNull() throws Exception {
		sort().field(null, ASCENDING);
	}

	@Test(expected = IllegalArgumentException.class)
	public void needAtLeastOneField() throws Exception {
		sort().getDBObject();
	}

	private void assertField(SortOperation sortOperation, int position, String field, int order) {
		DBObject sortDbObject = (DBObject) sortOperation.getDBObject().get("$sort");
		List<String> keyList = new ArrayList<String>(sortDbObject.keySet());
		assertThat(sortDbObject.get(keyList.get(position)), is((Object) order));
	}
}
