/*
 * Copyright 2011 the original author or authors.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.WriteConcern;

/**
 * Integration test of the WriteConcern features for {@link MongoTemplate}.
 * 
 * @author Thomas Risberg
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:infrastructure.xml")
public class MongoTemplateWriteConcernTests {

	@Autowired
	MongoTemplate template;

	@Before
	public void setUp() {
		template.dropCollection(template.getDefaultCollectionName());
	}

	@After
	public void tearDown() {
		template.setCollectionWriteConcern(template.getDefaultCollectionName(), WriteConcern.NORMAL);
		template.setDatabaseWriteConcern(WriteConcern.NORMAL);
	}

	@Test
	public void testRetrievingTheDatabaseWriteConcern() throws Exception {
		WriteConcern wc = template.getDatabaseWriteConcern();
		assertNotNull(wc);
	}

	@Test
	public void testRetrievingTheCollectionWriteConcern() throws Exception {
		WriteConcern wc = template.getCollectionWriteConcern(template.getDefaultCollectionName());
		assertNotNull(wc);
	}

	@Test
	public void testSettingTheDatabaseWriteConcern() throws Exception {
		WriteConcern wc = template.getDatabaseWriteConcern();
		WriteConcern safe = WriteConcern.SAFE;
		template.setDatabaseWriteConcern(safe);
		assertEquals(safe.getW(), template.getDatabaseWriteConcern().getW());
		assertNotSame(wc.getW(), template.getDatabaseWriteConcern().getW());
	}

	@Test
	public void testSettingTheCollectionWriteConcern() throws Exception {
		String coll = template.getDefaultCollectionName();
		WriteConcern replicasSafe = WriteConcern.REPLICAS_SAFE;
		WriteConcern fsyncSafe = WriteConcern.FSYNC_SAFE;
		template.setDatabaseWriteConcern(fsyncSafe);
		template.setCollectionWriteConcern(coll, replicasSafe);
		assertEquals(replicasSafe.getW(), template.getCollectionWriteConcern(coll).getW());
		assertEquals(replicasSafe.fsync(), template.getCollectionWriteConcern(coll).fsync());
	}
}
