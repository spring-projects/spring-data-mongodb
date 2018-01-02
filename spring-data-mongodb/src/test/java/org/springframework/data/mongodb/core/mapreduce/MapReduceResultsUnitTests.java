/*
 * Copyright 2012-2018 the original author or authors.
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

import org.bson.Document;
import org.junit.Test;

/**
 * Unit tests for {@link MapReduceResults}.
 *
 * @author Oliver Gierke
 */
public class MapReduceResultsUnitTests {

	@Test // DATAMONGO-428
	public void resolvesOutputCollectionForPlainResult() {

		Document rawResult = new Document("result", "FOO");
		MapReduceResults<Object> results = new MapReduceResults<Object>(Collections.emptyList(), rawResult);

		assertThat(results.getOutputCollection(), is("FOO"));
	}

	@Test // DATAMONGO-428
	public void resolvesOutputCollectionForDocumentResult() {

		Document rawResult = new Document("result", new Document("collection", "FOO"));
		MapReduceResults<Object> results = new MapReduceResults<Object>(Collections.emptyList(), rawResult);

		assertThat(results.getOutputCollection(), is("FOO"));
	}

	@Test // DATAMONGO-378
	public void handlesLongTotalInResult() {

		Document inner = new Document("total", 1L);
		inner.put("mapTime", 1L);
		inner.put("emitLoop", 1);

		Document source = new Document("timing", inner);
		new MapReduceResults<Object>(Collections.emptyList(), source);
	}

	@Test // DATAMONGO-378
	public void handlesLongResultsForCounts() {

		Document inner = new Document("input", 1L);
		inner.put("emit", 1L);
		inner.put("output", 1);

		Document source = new Document("counts", inner);
		new MapReduceResults<Object>(Collections.emptyList(), source);
	}
}
