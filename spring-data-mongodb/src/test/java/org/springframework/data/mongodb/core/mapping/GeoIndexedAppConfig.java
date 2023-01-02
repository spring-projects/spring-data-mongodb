/*
 * Copyright 2011-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.mapping;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.core.mapping.event.LoggingEventListener;
import org.springframework.data.mongodb.test.util.MongoClientClosingTestConfiguration;
import org.springframework.data.mongodb.test.util.MongoTestUtils;

import com.mongodb.client.MongoClient;

public class GeoIndexedAppConfig extends MongoClientClosingTestConfiguration {

	public static String GEO_DB = "database";
	public static String GEO_COLLECTION = "geolocation";

	@Override
	public String getDatabaseName() {
		return GEO_DB;
	}

	@Override
	@Bean
	public MongoClient mongoClient() {
		return MongoTestUtils.client();
	}

	@Override
	protected Collection<String> getMappingBasePackages() {
		return Collections.singleton("org.springframework.data.mongodb.core.core.mapping");
	}

	@Bean
	public LoggingEventListener mappingEventsListener() {
		return new LoggingEventListener();
	}

	@Override
	protected Set<Class<?>> getInitialEntitySet() throws ClassNotFoundException {
		return Collections.emptySet();
	}

	@Override
	protected boolean autoIndexCreation() {
		return true;
	}
}
