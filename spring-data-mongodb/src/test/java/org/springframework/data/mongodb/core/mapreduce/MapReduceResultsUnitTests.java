/*
 * Copyright 2012 the original author or authors.
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
package org.springframework.data.mongodb.core.mapreduce;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.Collections;

import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Unit tests for {@link MapReduceResults}.
 * 
 * @author Oliver Gierke
 */
public class MapReduceResultsUnitTests {

	/**
	 * @see DATAMONGO-428
	 */
	@Test
	public void resolvesOutputCollectionForPlainResult() {

		DBObject rawResult = new BasicDBObject("result", "FOO");
		MapReduceResults<Object> results = new MapReduceResults<Object>(Collections.emptyList(), rawResult);

		assertThat(results.getOutputCollection(), is("FOO"));
	}

	/**
	 * @see DATAMONGO-428
	 */
	@Test
	public void resolvesOutputCollectionForDBObjectResult() {

		DBObject rawResult = new BasicDBObject("result", new BasicDBObject("collection", "FOO"));
		MapReduceResults<Object> results = new MapReduceResults<Object>(Collections.emptyList(), rawResult);

		assertThat(results.getOutputCollection(), is("FOO"));
	}

	/**
	 * @see DATAMONGO-378
	 */
	@Test
	public void handlesLongTotalInResult() {

		DBObject inner = new BasicDBObject("total", 1L);
		inner.put("mapTime", 1L);
		inner.put("emitLoop", 1);

		DBObject source = new BasicDBObject("timing", inner);
		new MapReduceResults<Object>(Collections.emptyList(), source);
	}

	/**
	 * @see DATAMONGO-378
	 */
	@Test
	public void handlesLongResultsForCounts() {

		DBObject inner = new BasicDBObject("input", 1L);
		inner.put("emit", 1L);
		inner.put("output", 1);

		DBObject source = new BasicDBObject("counts", inner);
		new MapReduceResults<Object>(Collections.emptyList(), source);
	}
}
