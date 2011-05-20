/*
 * Copyright 2010-2011 the original author or authors.
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
package org.springframework.data.document.mongodb;

import com.mongodb.Mongo;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.document.mongodb.config.AbstractMongoConfiguration;
import org.springframework.data.document.mongodb.mapping.event.LoggingEventListener;
import org.springframework.data.document.mongodb.mapping.event.MongoMappingEvent;

@Configuration
public class GeoSpatialAppConfig extends AbstractMongoConfiguration {

	@Override
	public String defaultDatabaseName() {
		return "geospatial";
	}

	@Bean
	public Mongo mongo() throws Exception {
		return new Mongo("localhost");
	}

	@Bean
	public MongoTemplate mongoTemplate() throws Exception {
		return new MongoTemplate(mongoDbFactory());
	}

	@Bean
	public LoggingEventListener<MongoMappingEvent> mappingEventsListener() {
		return new LoggingEventListener<MongoMappingEvent>();
	}

	@Override
	public String mappingBasePackage() {
		return "org.springframework.data.document.mongodb";
	}

}
