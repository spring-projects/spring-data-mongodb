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

import org.junit.Test;
import org.springframework.data.mongodb.core.DBObjectUtils;

import com.mongodb.DBObject;

/**
 * Unit tests for {@link ProjectionOperation}.
 * 
 * @author Oliver Gierke
 */
public class ProjectionOperationUnitTests {

	static final String PROJECT = "$project";

	@Test
	public void declaresBackReferenceCorrectly() {

		ProjectionOperation operation = new ProjectionOperation();
		operation = operation.and("prop").backReference();

		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		DBObject projectClause = DBObjectUtils.getAsDBObject(dbObject, PROJECT);
		assertThat(projectClause.get("prop"), is((Object) Fields.UNDERSCORE_ID_REF));
	}

	@Test
	public void usesOneForImplicitTarget() {

		ProjectionOperation operation = new ProjectionOperation(Fields.fields("foo").and("bar", "foobar"));

		DBObject dbObject = operation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		DBObject projectClause = DBObjectUtils.getAsDBObject(dbObject, PROJECT);

		System.out.println(projectClause);

		assertThat(projectClause.get("foo"), is((Object) "$foo"));
		assertThat(projectClause.get("bar"), is((Object) "$foobar"));
	}
}
