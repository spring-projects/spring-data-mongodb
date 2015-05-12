/*
 * Copyright 2012-2015 the original author or authors.
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
package org.springframework.data.mongodb.core.index;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.hamcrest.Matchers;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.test.util.CleanMongoDB;
import org.springframework.data.mongodb.test.util.MongoVersionRule;
import org.springframework.data.util.Version;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Integration tests for {@link MongoPersistentEntityIndexCreator}.
 * 
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Thomas Darimont
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class MongoPersistentEntityIndexCreatorIntegrationTests {

	static final String SAMPLE_TYPE_COLLECTION_NAME = "sampleEntity";
	static final String RECURSIVE_TYPE_COLLECTION_NAME = "recursiveGenericTypes";

	public static @ClassRule RuleChain rules = RuleChain.outerRule(MongoVersionRule.atLeast(new Version(2, 6))).around(
			CleanMongoDB.indexes(Arrays.asList(SAMPLE_TYPE_COLLECTION_NAME, RECURSIVE_TYPE_COLLECTION_NAME)));

	@Autowired @Qualifier("mongo1") MongoOperations templateOne;

	@Autowired @Qualifier("mongo2") MongoOperations templateTwo;

	@Test
	public void createsIndexForConfiguredMappingContextOnly() {

		List<IndexInfo> indexInfo = templateOne.indexOps(SampleEntity.class).getIndexInfo();
		assertThat(indexInfo, hasSize(greaterThan(0)));
		assertThat(indexInfo, Matchers.<IndexInfo> hasItem(hasProperty("name", is("prop"))));

		indexInfo = templateTwo.indexOps(SAMPLE_TYPE_COLLECTION_NAME).getIndexInfo();
		assertThat(indexInfo, hasSize(0));
	}

	/**
	 * @see DATAMONGO-1202
	 */
	@Test
	public void shouldHonorIndexedPropertiesWithRecursiveMappings() {

		List<IndexInfo> indexInfo = templateOne.indexOps(RecursiveConcreteType.class).getIndexInfo();

		assertThat(indexInfo, hasSize(greaterThan(0)));
		assertThat(indexInfo, Matchers.<IndexInfo> hasItem(hasProperty("name", is("firstName"))));
	}

	@Document(collection = RECURSIVE_TYPE_COLLECTION_NAME)
	static abstract class RecursiveGenericType<RGT extends RecursiveGenericType<RGT>> {

		@Id Long id;

		@org.springframework.data.mongodb.core.mapping.DBRef RGT referrer;

		@Indexed String firstName;

		public RecursiveGenericType(Long id, String firstName, RGT referrer) {
			this.firstName = firstName;
			this.id = id;
			this.referrer = referrer;
		}
	}

	static class RecursiveConcreteType extends RecursiveGenericType<RecursiveConcreteType> {

		public RecursiveConcreteType(Long id, String firstName, RecursiveConcreteType referrer) {
			super(id, firstName, referrer);
		}
	}
}
