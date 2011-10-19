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
package org.springframework.data.mongodb.gridfs;

import static org.hamcrest.CoreMatchers.*;
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
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSFile;

/**
 * Integration tests for {@link GridFsTemplate}.
 * 
 * @author Oliver Gierke
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

	private static void assertSame(GridFSFile left, GridFSFile right) {

		assertThat(left.getId(), is(right.getId()));
		assertThat(left.getMD5(), is(right.getMD5()));
		assertThat(left.getMetaData(), is(right.getMetaData()));
	}

	class Metadata {

		String version;
	}
}
