/*
 * Copyright 2011-2013 the original author or authors.
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
package org.springframework.data.mongodb.gridfs;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.springframework.data.mongodb.core.query.Query.*;
import static org.springframework.data.mongodb.gridfs.GridFsCriteria.*;

import java.io.IOException;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSFile;

/**
 * Integration tests for {@link GridFsTemplate}.
 * 
 * @author Oliver Gierke
 * @author Philipp Schneider
 * @author Aparna Chaudhary
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:gridfs/gridfs.xml")
public class GridFsTemplateIIntegrationTests {

	Resource resource = new ClassPathResource("gridfs/gridfs.xml");

	@Autowired
	GridFsOperations operations;

	@Before
	public void setUp() {
		operations.delete(null);
	}

	@Test
	public void storesAndFindsSimpleDocument() throws IOException {

		GridFSFile reference = operations.store(resource.getInputStream(), "foo.xml");

		List<GridFSDBFile> result = operations.find(null);
		assertThat(result.size(), is(1));
		assertSame(result.get(0), reference);
	}

	@Test
	public void writesMetadataCorrectly() throws IOException {

		DBObject metadata = new BasicDBObject("key", "value");
		GridFSFile reference = operations.store(resource.getInputStream(), "foo.xml", metadata);

		List<GridFSDBFile> result = operations.find(query(whereMetaData("key").is("value")));
		assertThat(result.size(), is(1));
		assertSame(result.get(0), reference);
	}

	/**
	 * @see DATAMONGO-503
	 */
	@Test
	public void storesContentType() throws IOException {

		GridFSFile reference = operations.store(resource.getInputStream(), "foo2.xml", "application/xml");

		List<GridFSDBFile> result = operations.find(query(whereContentType().is("application/xml")));
		assertThat(result.size(), is(1));
		assertSame(result.get(0), reference);
	}

	@Test
	public void marshalsComplexMetadata() throws IOException {

		Metadata metadata = new Metadata();
		metadata.version = "1.0";

		GridFSFile reference = operations.store(resource.getInputStream(), "foo.xml", metadata);
		List<GridFSDBFile> result = operations.find(query(whereFilename().is("foo.xml")));
		assertThat(result.size(), is(1));
		assertSame(result.get(0), reference);
	}

	@Test
	public void findsFilesByResourcePattern() throws IOException {

		GridFSFile reference = operations.store(resource.getInputStream(), "foo.xml");

		GridFsResource[] resources = operations.getResources("*.xml");
		assertThat(resources.length, is(1));
		assertThat(resources[0].getId(), is(reference.getId()));
		assertThat(resources[0].contentLength(), is(reference.getLength()));
		assertThat(resources[0].getContentType(), is(reference.getContentType()));
	}

	@Test
	public void findsFilesByResourceLocation() throws IOException {

		GridFSFile reference = operations.store(resource.getInputStream(), "foo.xml");

		GridFsResource[] resources = operations.getResources("foo.xml");
		assertThat(resources.length, is(1));
		assertThat(resources[0].getId(), is(reference.getId()));
		assertThat(resources[0].contentLength(), is(reference.getLength()));
		assertThat(resources[0].getContentType(), is(reference.getContentType()));
	}

	/**
	 * @see DATAMONGO-534
	 */
	@Test
	public void considersSortWhenQueryingFiles() throws IOException {

		GridFSFile second = operations.store(resource.getInputStream(), "foo.xml");
		GridFSFile third = operations.store(resource.getInputStream(), "foobar.xml");
		GridFSFile first = operations.store(resource.getInputStream(), "bar.xml");

		Query query = new Query().with(new Sort(Direction.ASC, "filename"));

		List<GridFSDBFile> result = operations.find(query);
		assertThat(result, hasSize(3));
		assertSame(result.get(0), first);
		assertSame(result.get(1), second);
		assertSame(result.get(2), third);
	}

	/**
	 * @see DATAMONGO-534
	 */
	@Test
	public void queryingWithNullQueryReturnsAllFiles() throws IOException {

		GridFSFile reference = operations.store(resource.getInputStream(), "foo.xml");

		List<GridFSDBFile> result = operations.find(null);

		assertThat(result, hasSize(1));
		assertSame(result.get(0), reference);
	}

	/**
	 * Tests index creation with default bucket
	 */
	@Test
	public void testEnsureIndex() {

		// when
		operations.indexOps().ensureIndex(new Index().on("md5", Direction.ASC).unique());

		// then
		DBCollection coll = operations.getFilesCollection();
		List<DBObject> indexInfo = coll.getIndexInfo();
		assertThat(indexInfo.size(), is(3));
		String indexKey = null;
		boolean unique = false;
		for (DBObject ix : indexInfo) {
			if ("md5_1".equals(ix.get("name"))) {
				indexKey = ix.get("key").toString();
				unique = (Boolean) ix.get("unique");
			}
		}
		assertThat(indexKey, is("{ \"md5\" : 1}"));
		assertThat(unique, is(true));
	}

	/**
	 * Tests index deletion with default bucket
	 */
	@Test
	public void testDropIndex() {

		// given
		operations.indexOps().ensureIndex(new Index().on("md5", Direction.ASC).unique());

		DBCollection coll = operations.getFilesCollection();
		List<DBObject> indexInfo = coll.getIndexInfo();
		assertThat(indexInfo.size(), is(3));

		// when
		operations.indexOps().dropIndex("md5_1");

		// then
		indexInfo = coll.getIndexInfo();
		assertThat(indexInfo.size(), is(2));
	}

	private static void assertSame(GridFSFile left, GridFSFile right) {

		assertThat(left.getId(), is(right.getId()));
		assertThat(left.getMD5(), is(right.getMD5()));
		assertThat(left.getMetaData(), is(right.getMetaData()));
	}

	class Metadata {

		String version;
	}
}
