/*
 * Copyright 2020. the original author or authors.
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

/*
 * Copyright 2020 the original author or authors.
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
package org.springframework.data.mongodb.core.staticmetadata;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import java.util.Arrays;
import java.util.LinkedHashSet;

import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.buildtimetypeinfo.Address;
import org.springframework.data.mongodb.buildtimetypeinfo.AddressTypeInformation;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.mongodb.buildtimetypeinfo.Person;
import org.springframework.data.mongodb.buildtimetypeinfo.PersonTypeInformation;

import com.mongodb.client.MongoClients;

/**
 * @author Christoph Strobl
 * @since 2020/10
 */
public class StaticMetadataTests {

	MongoMappingContext mappingContext;
	MappingMongoConverter mongoConverter;
	MongoTemplate template;

	Person luke;

	@BeforeAll
	static void beforeAll() {
		ClassTypeInformation.warmCache(PersonTypeInformation.instance(), AddressTypeInformation.instance());
	}

	@BeforeEach
	void beforeEach() {

		mappingContext = new MongoMappingContext();
		mappingContext.setInitialEntitySet(new LinkedHashSet<>(
				Arrays.asList(Person.class, Address.class)));
		mappingContext.initialize();

		mongoConverter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, mappingContext);
		mongoConverter.afterPropertiesSet();

		template = new MongoTemplate(new SimpleMongoClientDatabaseFactory(MongoClients.create(), "sem"), mongoConverter);

		luke = new Person("luke", "skywalker");
		luke.setAddress(new Address("Mos Eisley", "WB154"));
		luke.setAge(22);
		luke = luke.withId(9876);
		luke.setNicknames(Arrays.asList("jedi", "wormie"));
	}

	@Test
	void readWrite() {

		template.save(luke);

		Document savedDocument = template.execute("star-wars",
				collection -> collection.find(new Document("_id", luke.getId())).first());
		System.out.println("savedDocument.toJson(): " + savedDocument.toJson());

		Person savedEntity = template.findOne(query(where("id").is(luke.getId())), Person.class);
		System.out.println("savedEntity: " + savedEntity);

		assertThat(savedEntity).isEqualTo(luke);
	}

}
