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

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.openjdk.jmh.results.RunResult;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.util.JSON;

/**
 * MongoDB specific {@link ResultsWriter} implementation.
 *
 * @author Christoph Strobl
 * @since 2.0
 */
class MongoResultsWriter implements ResultsWriter {

	private final String uri;

	MongoResultsWriter(String uri) {
		this.uri = uri;
	}

	@Override
	public void write(Collection<RunResult> results) {

		Date now = new Date();
		StandardEnvironment env = new StandardEnvironment();

		String projectVersion = env.getProperty("project.version", "unknown");

		MongoClientURI uri = new MongoClientURI(this.uri);
		MongoClient client = null;

		try {
			client = new MongoClient(uri);
		} catch (UnknownHostException e) {
			throw new RuntimeException(e);
		}

		String dbName = StringUtils.hasText(uri.getDatabase()) ? uri.getDatabase() : "spring-data-mongodb-benchmarks";
		DB db = client.getDB(dbName);

		for (BasicDBObject dbo : (List<BasicDBObject>) JSON.parse(Utils.jsonifyResults(results))) {

			String collectionName = extractClass(dbo.get("benchmark").toString());

			BasicDBObject sink = new BasicDBObject();
			sink.append("_version", projectVersion);
			sink.append("_method", extractBenchmarkName(dbo.get("benchmark").toString()));
			sink.append("_date", now);
			sink.append("_snapshot", projectVersion.toLowerCase().contains("snapshot"));

			sink.putAll(dbo.toMap());

			db.getCollection(collectionName).insert(fixDocumentKeys(sink));
		}

		client.close();

	}

	/**
	 * Replace {@code .} by {@code ,}.
	 *
	 * @param doc
	 * @return
	 */
	private BasicDBObject fixDocumentKeys(BasicDBObject doc) {

		BasicDBObject sanitized = new BasicDBObject();

		for (Object key : doc.keySet()) {

			Object value = doc.get(key);
			if (value instanceof BasicDBObject) {
				value = fixDocumentKeys((BasicDBObject) value);
			}

			if (key instanceof String) {

				String newKey = (String) key;
				if (newKey.contains(".")) {
					newKey = newKey.replace('.', ',');
				}

				sanitized.put(newKey, value);
			} else {
				sanitized.put(ObjectUtils.nullSafeToString(key).replace('.', ','), value);
			}
		}

		return sanitized;
	}

	private String extractClass(String source) {

		String tmp = source.substring(0, source.lastIndexOf('.'));
		return tmp.substring(tmp.lastIndexOf(".") + 1);
	}

	private String extractBenchmarkName(String source) {
		return source.substring(source.lastIndexOf(".") + 1);
	}

}
