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
package org.springframework.data.mongodb.microbenchmark;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.util.Collection;

import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.results.format.ResultFormatFactory;
import org.openjdk.jmh.results.format.ResultFormatType;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
interface ResultsWriter {

	/**
	 * Write the {@link RunResult}s.
	 *
	 * @param results can be {@literal null}.
	 */
	void write(Collection<RunResult> results);

	/* non Java8 hack */
	class Utils {

		/**
		 * Get the uri specific {@link ResultsWriter}.
		 *
		 * @param uri must not be {@literal null}.
		 * @return
		 */
		static ResultsWriter forUri(String uri) {
			return uri.startsWith("mongodb:") ? new MongoResultsWriter(uri) : new HttpResultsWriter(uri);
		}

		/**
		 * Convert {@link RunResult}s to JMH Json representation.
		 *
		 * @param results
		 * @return json string representation of results.
		 * @see org.openjdk.jmh.results.format.JSONResultFormat
		 */
		static String jsonifyResults(Collection<RunResult> results) {

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ResultFormatFactory.getInstance(ResultFormatType.JSON, new PrintStream(baos)).writeOut(results);
			return new String(baos.toByteArray(), Charset.forName("UTF-8"));
		}
	}

}
