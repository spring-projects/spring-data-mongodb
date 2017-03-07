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

import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.util.Collection;

import org.openjdk.jmh.results.RunResult;
import org.springframework.util.CollectionUtils;

/**
 * {@link ResultsWriter} implementation of {@link URLConnection}.
 *
 * @since 2.0
 */
class HttpResultsWriter implements ResultsWriter {

	private final String url;

	HttpResultsWriter(String url) {
		this.url = url;
	}

	@Override
	public void write(Collection<RunResult> results) {

		if (CollectionUtils.isEmpty(results)) {
			return;
		}

		try {

			URLConnection connection = new URL(url).openConnection();
			connection.setConnectTimeout(1000);
			connection.setDoOutput(true);
			connection.setRequestProperty("Content-Type", "application/json");

			OutputStream output = null;
			try {
				output = connection.getOutputStream();
				output.write(ResultsWriter.Utils.jsonifyResults(results).getBytes(Charset.forName("UTF-8")));
			} finally {
				if (output != null) {
					output.close();
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
