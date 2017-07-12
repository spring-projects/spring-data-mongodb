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

import lombok.SneakyThrows;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.util.Collection;

import org.openjdk.jmh.results.RunResult;
import org.springframework.core.env.StandardEnvironment;
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
	@SneakyThrows
	public void write(Collection<RunResult> results) {

		if (CollectionUtils.isEmpty(results)) {
			return;
		}

		StandardEnvironment env = new StandardEnvironment();

		String projectVersion = env.getProperty("project.version", "unknown");
		String gitBranch = env.getProperty("git.branch", "unknown");
		String gitDirty = env.getProperty("git.dirty", "no");
		String gitCommitId = env.getProperty("git.commit.id", "unknown");

		HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
		connection.setConnectTimeout(1000);
		connection.setReadTimeout(1000);
		connection.setDoOutput(true);
		connection.setRequestMethod("POST");

		connection.setRequestProperty("Content-Type", "application/json");
		connection.addRequestProperty("X-Project-Version", projectVersion);
		connection.addRequestProperty("X-Git-Branch", gitBranch);
		connection.addRequestProperty("X-Git-Dirty", gitDirty);
		connection.addRequestProperty("X-Git-Commit-Id", gitCommitId);

		OutputStream output = null;
		try {
			output = connection.getOutputStream();
			output.write(ResultsWriter.Utils.jsonifyResults(results).getBytes(Charset.forName("UTF-8")));
		} finally {
			if (output != null) {
				output.close();
			}
		}

		if (connection.getResponseCode() >= 400) {
			throw new IllegalStateException(
					String.format("Status %d %s", connection.getResponseCode(), connection.getResponseMessage()));
		}
	}
}
