/*
 * Copyright 2011-2019 the original author or authors.
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
package org.springframework.data.mongodb.core.index;

import static org.springframework.data.mongodb.test.util.Assertions.*;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.MongoCollectionUtils;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.config.AbstractMongoClientConfiguration;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.test.util.MongoTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.client.MongoClient;

/**
 * Integration tests for index handling.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Jordi Llach
 * @author Mark Paluch
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class IndexingIntegrationTests {

	@Autowired MongoOperations operations;
	@Autowired MongoDbFactory mongoDbFactory;
	@Autowired ConfigurableApplicationContext context;

	@Configuration
	static class Config extends AbstractMongoClientConfiguration {

		@Override
		public MongoClient mongoClient() {
			return MongoTestUtils.client();
		}

		@Override
		protected String getDatabaseName() {
			return "database";
		}

		@Bean
		TimeoutResolver myTimeoutResolver() {
			return new TimeoutResolver("11s");
		}
	}

	@After
	public void tearDown() {
		operations.dropCollection(IndexedPerson.class);
	}

	@Test // DATAMONGO-237
	@DirtiesContext
	public void createsIndexWithFieldName() {

		operations.getConverter().getMappingContext().getPersistentEntity(IndexedPerson.class);

		assertThat(hasIndex("_firstname", IndexedPerson.class)).isTrue();
	}

	@Test // DATAMONGO-2188
	@DirtiesContext
	public void shouldNotCreateIndexOnIndexingDisabled() {

		MongoMappingContext context = new MongoMappingContext();
		context.setAutoIndexCreation(false);

		MongoTemplate template = new MongoTemplate(mongoDbFactory,
				new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, context));

		template.getConverter().getMappingContext().getPersistentEntity(IndexedPerson.class);

		assertThat(hasIndex("_firstname", MongoCollectionUtils.getPreferredCollectionName(IndexedPerson.class))).isFalse();
	}

	@Test // DATAMONGO-1163
	@DirtiesContext
	public void createsIndexFromMetaAnnotation() {

		operations.getConverter().getMappingContext().getPersistentEntity(IndexedPerson.class);

		assertThat(hasIndex("_lastname", IndexedPerson.class)).isTrue();
	}

	@Test // DATAMONGO-2112
	@DirtiesContext
	public void evaluatesTimeoutSpelExpresssionWithBeanReference() {

		operations.getConverter().getMappingContext().getPersistentEntity(WithSpelIndexTimeout.class);

		Optional<org.bson.Document> indexInfo = operations.execute("withSpelIndexTimeout", collection -> {

			return collection.listIndexes(org.bson.Document.class).into(new ArrayList<>()) //
					.stream() //
					.filter(it -> it.get("name").equals("someString")) //
					.findFirst();
		});

		assertThat(indexInfo).isPresent();
		assertThat(indexInfo.get()).containsEntry("expireAfterSeconds", 11L);
	}

	@Target({ ElementType.FIELD })
	@Retention(RetentionPolicy.RUNTIME)
	@Indexed
	@interface IndexedFieldAnnotation {
	}

	@Document
	class IndexedPerson {

		@Field("_firstname") @Indexed String firstname;
		@Field("_lastname") @IndexedFieldAnnotation String lastname;
	}

	@RequiredArgsConstructor
	@Getter
	static class TimeoutResolver {
		final String timeout;
	}

	@Document
	class WithSpelIndexTimeout {
		@Indexed(expireAfter = "#{@myTimeoutResolver?.timeout}") String someString;
	}

	/**
	 * Returns whether an index with the given name exists for the given entity type.
	 *
	 * @param indexName
	 * @param entityType
	 * @return
	 */
	private boolean hasIndex(String indexName, Class<?> entityType) {
		return hasIndex(indexName, operations.getCollectionName(entityType));
	}

	/**
	 * Returns whether an index with the given name exists for the given collection.
	 *
	 * @param indexName
	 * @param collectionName
	 * @return
	 */
	private boolean hasIndex(String indexName, String collectionName) {

		return operations.execute(collectionName, collection -> {

			List<org.bson.Document> indexes = new ArrayList<>();
			collection.listIndexes(org.bson.Document.class).into(indexes);

			for (org.bson.Document indexInfo : indexes) {
				if (indexName.equals(indexInfo.get("name"))) {
					return true;
				}
			}
			return false;
		});
	}
}
