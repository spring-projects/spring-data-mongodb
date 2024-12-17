/*
 * Copyright 2024. the original author or authors.
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
 * Copyright 2024 the original author or authors.
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

import static org.springframework.data.mongodb.test.util.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.data.annotation.Version;
import org.springframework.data.auditing.IsNewAwareAuditingHandler;
import org.springframework.data.mapping.context.PersistentEntities;
import org.springframework.data.mongodb.core.MongoTemplateTests.PersonWithIdPropertyOfTypeUUIDListener;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.MongoClientExtension;
import org.springframework.data.mongodb.test.util.MongoTestTemplate;

import com.mongodb.client.MongoClient;

/**
 * @author Christoph Strobl
 */
@ExtendWith(MongoClientExtension.class)
public class MongoTemplateBulkTests {

	public static final String DB_NAME = "mongo-template-bulk-tests";

	static @Client MongoClient client;

	ConfigurableApplicationContext context = new GenericApplicationContext();

	MongoTestTemplate template = new MongoTestTemplate(cfg -> {

		cfg.configureDatabaseFactory(it -> {

			it.client(client);
			it.defaultDb(DB_NAME);
		});

		cfg.configureMappingContext(it -> {
			it.autocreateIndex(false);
			it.initialEntitySet(AuditablePerson.class);
		});

		cfg.configureApplicationContext(it -> {
			it.applicationContext(context);
			it.addEventListener(new PersonWithIdPropertyOfTypeUUIDListener());
		});

		cfg.configureAuditing(it -> {
			it.auditingHandler(ctx -> {
				return new IsNewAwareAuditingHandler(PersistentEntities.of(ctx));
			});
		});
	});

	@BeforeEach
	void beforeEach() {
		template.flush(SimpleEntity.class);
	}

	@Test
	void justSimpleNew() {

		List<SimpleEntity> entities = simpleEntities(5);
		template.saveAll(entities);

		template.verify().collection(SimpleEntity.class).hasSize(5).documentsSatisfy(document -> {
			assertThat(document) //
					.hasEntrySatisfying("_id", value -> assertThat(value).isInstanceOf(ObjectId.class)) //
					.hasEntrySatisfying("name",
							value -> assertThat(value).asInstanceOf(InstanceOfAssertFactories.STRING).startsWith("name-"));
		});

		assertThat(entities).map(SimpleEntity::getId).allMatch(ObjectId::isValid);
	}

	@Test
	void justSimpleReplace() {

		List<SimpleEntity> entities = simpleEntities(5).stream()
				.peek(entity -> entity.id = "%s".formatted(entity.name.replace("name", "id"))).collect(Collectors.toList());
		template.saveAll(entities);

		template.verify().collection(SimpleEntity.class).hasSize(5).documentsSatisfy(document -> {
			assertThat(document) //
					.hasEntrySatisfying("_id", value -> assertThat(value).isInstanceOf(String.class)) //
					.hasEntrySatisfying("name",
							value -> assertThat(value).asInstanceOf(InstanceOfAssertFactories.STRING).startsWith("name-"));
		});
	}

	@Test
	void mixedNewReplace() {
		int i = 0;

		List<SimpleEntity> entities = simpleEntities(5);
		for (SimpleEntity entity : entities) {
			if (i % 2 == 0) {
				entity.id = "%s".formatted(entity.name.replace("name", "id"));
			}
			i++;
		}
		template.saveAll(entities);

		template.verify().collection(SimpleEntity.class).documents().atPosition(0).satisfies(document -> {
			assertThat(document.get("_id")).isInstanceOf(String.class); //
		}).atPosition(1).satisfies(document -> {
			assertThat(document.get("_id")).isInstanceOf(ObjectId.class); //
		}).atPosition(2).satisfies(document -> {
			assertThat(document.get("_id")).isInstanceOf(String.class); //
		});
	}

	@Test
	void replaceExisting() {
		int i = 0;

		SimpleEntity e1 = new SimpleEntity();
		e1.id = "id-1";
		e1.name = "name-1";

		SimpleEntity e2 = new SimpleEntity();
		e2.id = "id-2";
		e2.name = "name-2";

		template.saveAll(List.of(e1, e2));

		e1.name = "name-11";

		template.saveAll(List.of(e1, e2));

		template.verify().collection(SimpleEntity.class).documents().hasSize(2) //
			.atPosition(0).satisfies(document -> {
			assertThat(document.get("name")).isEqualTo("name-11"); //
		}).atPosition(1).satisfies(document -> {
			assertThat(document.get("name")).isEqualTo("name-2");//
		});
	}

	List<SimpleEntity> simpleEntities(int count) {

		List<SimpleEntity> entities = new ArrayList<>(count);

		for (int i = 0; i < count; i++) {
			SimpleEntity simpleEntity = new SimpleEntity();
			simpleEntity.name = "name-%s".formatted(i);
			entities.add(simpleEntity);
		}
		return entities;
	}

	@Document("simple")
	static class SimpleEntity {

		String id;
		String name;

		public String getId() {
			return id;
		}
	}

	@Document("versioned")
	static class VersionedEntity {

		String id;
		@Version Long version;
		String name;
	}

}
