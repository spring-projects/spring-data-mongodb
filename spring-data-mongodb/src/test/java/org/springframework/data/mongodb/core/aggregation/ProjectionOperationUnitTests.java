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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.junit.Test;
import org.springframework.data.mongodb.core.DBObjectUtils;

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;

/**
 * Unit tests for {@link ProjectionOperation}.
 * 
 * @author Oliver Gierke
 */
public class ProjectionOperationUnitTests {

	static final String PROJECT = "$project";

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNullFields() {
		new ProjectionOperation(null);
	}

	@Test
	public void declaresBackReferenceCorrectly() {

		ProjectionOperation operation = new ProjectionOperation();
		operation = operation.and("prop").previousOperation();

		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		DBObject projectClause = DBObjectUtils.getAsDBObject(dbObject, PROJECT);
		assertThat(projectClause.get("prop"), is((Object) Fields.UNDERSCORE_ID_REF));
	}

	@Test
	public void alwaysUsesExplicitReference() {

		ProjectionOperation operation = new ProjectionOperation(Fields.fields("foo").and("bar", "foobar"));

		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		DBObject projectClause = DBObjectUtils.getAsDBObject(dbObject, PROJECT);

		assertThat(projectClause.get("foo"), is((Object) "$foo"));
		assertThat(projectClause.get("bar"), is((Object) "$foobar"));
	}

	@Test
	public void aliasesSimpleFieldProjection() {

		ProjectionOperation operation = new ProjectionOperation();

		DBObject dbObject = operation.and("foo").as("bar").toDBObject(Aggregation.DEFAULT_CONTEXT);
		DBObject projectClause = DBObjectUtils.getAsDBObject(dbObject, PROJECT);

		assertThat(projectClause.get("bar"), is((Object) "$foo"));
	}

	@Test
	public void aliasesArithmeticProjection() {

		ProjectionOperation operation = new ProjectionOperation();

		DBObject dbObject = operation.and("foo").plus(41).as("bar").toDBObject(Aggregation.DEFAULT_CONTEXT);
		DBObject projectClause = DBObjectUtils.getAsDBObject(dbObject, PROJECT);
		DBObject barClause = DBObjectUtils.getAsDBObject(projectClause, "bar");
		BasicDBList addClause = DBObjectUtils.getAsDBList(barClause, "$add");

		assertThat(addClause, hasSize(2));
		assertThat(addClause.get(0), is((Object) "$foo"));
		assertThat(addClause.get(1), is((Object) 41));
	}
}
