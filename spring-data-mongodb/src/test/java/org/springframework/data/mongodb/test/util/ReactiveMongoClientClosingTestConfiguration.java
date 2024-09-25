/*
 * Copyright 2021-2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.test.util;

import javax.annotation.PreDestroy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.config.AbstractReactiveMongoConfiguration;
import org.springframework.test.util.ReflectionTestUtils;

import com.mongodb.reactivestreams.client.MongoClient;

/**
 * @author Christoph Strobl
 */
public abstract class ReactiveMongoClientClosingTestConfiguration extends AbstractReactiveMongoConfiguration {

	@Autowired(required = false) ReactiveMongoDatabaseFactory dbFactory;

	@PreDestroy
	public void destroy() {

		if (dbFactory != null) {
			Object mongo = ReflectionTestUtils.getField(dbFactory, "mongo");
			if (mongo != null) {
				((MongoClient) mongo).close();
			}
		}
	}
}
