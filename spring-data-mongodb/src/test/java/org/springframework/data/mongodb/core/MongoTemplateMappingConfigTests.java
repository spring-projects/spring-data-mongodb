/*
 * Copyright 2023 the original author or authors.
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
package org.springframework.data.mongodb.core;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.mongodb.core.mapping.MappingConfig.*;

import lombok.Data;

import java.util.List;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import com.mongodb.client.MongoClients;

/**
 * @author Christoph Strobl
 * @since 2023/06
 */
public class MongoTemplateMappingConfigTests {

	@Test
	void testProgrammaticMetadata() {

		SimpleMongoClientDatabaseFactory dbFactory = new SimpleMongoClientDatabaseFactory(MongoClients.create(),
				"test-manual-config");

		MongoMappingContext mappingContext = new MongoMappingContext();
		mappingContext.mappingRules(rules -> {
			rules.add(Sample.class, cfg -> {
				cfg.namespace("my-sample");
				cfg.entityCreator(args -> {
					return new Sample(args.get(Sample::getName));
				});
				cfg.define(Sample::getName, PropertyConfig::useAsId);
				cfg.define(Sample::getValue, property -> property.mappedName("va-l-ue"));
			});
		});
		mappingContext.afterPropertiesSet();

		MappingMongoConverter mappingMongoConverter = new MappingMongoConverter(dbFactory, mappingContext);
		mappingMongoConverter.afterPropertiesSet();

		MongoTemplate template = new MongoTemplate(dbFactory, mappingMongoConverter);
		template.dropCollection(Sample.class);

		Sample sample = new Sample("s1");
		sample.value = "val";
		template.save(sample);

		Document dbValue = template.execute("my-sample", collection -> {
			return collection.find(new Document()).first();
		});

		System.out.println("dbValue: " + dbValue);
		assertThat(dbValue).containsEntry("_id", sample.name).containsEntry("va-l-ue", sample.value);

		List<Sample> entries = template.find(Query.query(Criteria.where("name").is(sample.name)), Sample.class);
		entries.forEach(System.out::println);

		assertThat(entries).containsExactly(sample);
	}

	@Data
	@org.springframework.data.mongodb.core.mapping.Document(collection = "my-sample")
	static class Sample {

		Sample(String name) {
			this.name = name;
		}

		@Id final String name;

		@Field(name = "va-l-ue") String value;
	}

}
