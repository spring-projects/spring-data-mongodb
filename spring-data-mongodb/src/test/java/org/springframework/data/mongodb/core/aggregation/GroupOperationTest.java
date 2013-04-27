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
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.springframework.data.mongodb.core.aggregation.GroupOperation.group;
import static org.springframework.data.mongodb.core.aggregation.GroupOperation.IdField.idField;

import org.junit.Test;
import org.springframework.data.mongodb.core.aggregation.GroupOperation.IdField;

import com.mongodb.DBObject;

/**
 * Tests of {@link GroupOperation}.
 * 
 * @see DATAMONGO-586
 * @author Sebastian Herold
 */
public class GroupOperationTest {

	private static final String FIELD = "field";
	private static final String FIELD_WITH_PREFIX = "$" + FIELD;
	private static final String KEY = "key";

	private GroupOperation groupOperation;

	@Test
	public void addToSet() throws Exception {
		groupOperation = group("value").addToSet(KEY, FIELD);
		assertOperation(groupOperation, "$addToSet", KEY);
	}

	@Test
	public void firstOperation() throws Exception {
		groupOperation = group("value").first(KEY, FIELD);
		assertOperation(groupOperation, "$first", KEY);
	}

	@Test
	public void lastOperation() throws Exception {
		groupOperation = group("value").last(KEY, FIELD);
		assertOperation(groupOperation, "$last", KEY);
	}

	@Test
	public void maxOperation() throws Exception {
		groupOperation = group("value").max(KEY, FIELD);
		assertOperation(groupOperation, "$max", KEY);
	}

	@Test
	public void minOperation() throws Exception {
		groupOperation = group("value").min(KEY, FIELD);
		assertOperation(groupOperation, "$min", KEY);
	}

	@Test
	public void avgOperation() throws Exception {
		groupOperation = group("value").avg(KEY, FIELD);
		assertOperation(groupOperation, "$avg", KEY);
	}

	@Test
	public void pushOperation() throws Exception {
		groupOperation = group("value").push(KEY, FIELD);
		assertOperation(groupOperation, "$push", KEY);
	}

	@Test
	public void countFieldWithIncrement() throws Exception {
		groupOperation = group("value").count(KEY, 42);
		Object sumValue = getSumValue(groupOperation);
		assertThat(sumValue, is((Object) 42.0));
	}

	@Test
	public void countField() throws Exception {
		groupOperation = group("value").count(KEY);
		Object sumValue = getSumValue(groupOperation);
		assertThat(sumValue, is((Object) 1.0));
	}

	@Test
	public void sumField() throws Exception {
		groupOperation = group("value").sum(KEY, FIELD);
		Object sumValue = getSumValue(groupOperation);
		assertThat(sumValue, is((Object) FIELD_WITH_PREFIX));
	}

	@Test
	public void concatOperations() throws Exception {
		groupOperation = group("value").push(KEY, FIELD).avg("key2", FIELD).last("key3", FIELD);
		assertOperation(groupOperation, "$push", KEY);
		assertOperation(groupOperation, "$avg", "key2");
		assertOperation(groupOperation, "$last", "key3");
	}

	@Test
	public void createByFieldWithOperationPrefix() throws Exception {
		groupOperation = group("$value");
		assertThat(getId(groupOperation).toString(), is("$value"));
	}

	@Test(expected = IllegalArgumentException.class)
	public void idFieldsNotNull() throws Exception {
		group((IdField[]) null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void idFieldsNotEmpty() throws Exception {
		group();
	}

	@Test(expected = IllegalArgumentException.class)
	public void eachIdFieldNotNull() throws Exception {
		group(idField("value"), null, idField("value"));
	}

	@Test
	public void createByFieldWithoutOperationPrefix() throws Exception {
		groupOperation = group("value");
		assertThat(getId(groupOperation).toString(), is("$value"));
	}

	@Test
	public void idFieldRemovesOperationPrefix() throws Exception {
		IdField id = idField("$value");
		assertThat(id.getKey(), is("value"));
		assertThat(id.getValue(), is("$value"));
	}

	@Test
	public void idFieldAddsOperationPrefix() throws Exception {
		IdField id = idField("value");
		assertThat(id.getKey(), is("value"));
		assertThat(id.getValue(), is("$value"));
	}

	@Test(expected = IllegalArgumentException.class)
	public void idFieldNeedsAKey() throws Exception {
		idField(null, "validField");
	}

	@Test(expected = IllegalArgumentException.class)
	public void idFieldNeedsAValue() throws Exception {
		idField("validKey", null);
	}

	private Object getSumValue(GroupOperation groupOperation) {
		DBObject sumObject = (DBObject) getGroup(groupOperation).get(KEY);
		Object sumValue = sumObject.get("$sum");
		return sumValue;
	}

	private void assertOperation(GroupOperation groupOperation, String operation, String key) {
		DBObject groupObject = getGroup(groupOperation);
		assertThat(groupObject.get(key), is(notNullValue()));
		DBObject operationObject = (DBObject) groupObject.get(key);
		assertThat(operationObject.get(operation).toString(), is(FIELD_WITH_PREFIX));
	}

	private Object getId(GroupOperation groupOperation) {
		DBObject groupObject = getGroup(groupOperation);
		return groupObject.get("_id");
	}

	private DBObject getGroup(GroupOperation groupOperation) {
		DBObject groupObject = (DBObject) groupOperation.getDBObject().get("$group");
		assertThat(groupObject, is(notNullValue()));
		return groupObject;
	}
}
