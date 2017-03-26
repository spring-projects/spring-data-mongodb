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

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Unit tests for {@link SampleOperation}.
 * 
 * @author Gustavo de Geus
 */
public class SampleOperationUnitTests {

	private static final String SIZE = "size";
	private static final String OP = "$sample";

	@Test(expected = IllegalArgumentException.class)
	public void rejectsNegativeSample() {
		new SampleOperation(-1L);
	}

	@Test(expected = IllegalArgumentException.class)
	public void rejectsZeroSample() {
		new SampleOperation(0L);
	}
	
	@Test
	public void rendersSampleOperation() {
		long sampleSize = 5L;
		SampleOperation sampleOperation = Aggregation.sample(sampleSize);
		DBObject sampleOperationDbObject = sampleOperation.toDBObject(Aggregation.DEFAULT_CONTEXT);
		assertNotNull(sampleOperationDbObject.get(OP));
		assertThat(sampleOperationDbObject.get(OP), is(instanceOf(BasicDBObject.class)));
		BasicDBObject sampleSizeDbObject = (BasicDBObject) sampleOperationDbObject.get(OP);
		assertEquals(sampleSize, sampleSizeDbObject.get(SIZE));
	}
}
