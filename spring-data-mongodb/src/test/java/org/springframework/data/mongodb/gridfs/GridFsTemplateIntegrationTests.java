/*
 * Copyright 2011-2017 the original author or authors.
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
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;
import static org.springframework.data.mongodb.gridfs.GridFsCriteria.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.bson.BsonObjectId;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.client.gridfs.GridFSFindIterable;
import com.mongodb.gridfs.GridFSFile;

/**
 * Integration tests for {@link GridFsTemplate}.
 *
 * @author Oliver Gierke
 * @author Philipp Schneider
 * @author Thomas Darimont
 * @author Martin Baumgartner
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:gridfs/gridfs.xml")
public class GridFsTemplateIntegrationTests {

	Resource resource = new ClassPathResource("gridfs/gridfs.xml");

	@Autowired GridFsOperations operations;

	@Before
	public void setUp() {
		operations.delete(null);
	}

	@Test // DATAMONGO-6
	public void storesAndFindsSimpleDocument() throws IOException {

		ObjectId reference = operations.store(resource.getInputStream(), "foo.xml");

		List<com.mongodb.client.gridfs.model.GridFSFile> files = new ArrayList<com.mongodb.client.gridfs.model.GridFSFile>();
		GridFSFindIterable result = operations.find(query(where("_id").is(reference)));
		result.into(files);
		assertThat(files.size(), is(1));
		assertEquals(((BsonObjectId) files.get(0).getId()).getValue(), reference);
	}

	@Test // DATAMONGO-6
	public void writesMetadataCorrectly() throws IOException {

		Document metadata = new Document("key", "value");
		ObjectId reference = operations.store(resource.getInputStream(), "foo.xml", metadata);

		List<com.mongodb.client.gridfs.model.GridFSFile> files = new ArrayList<com.mongodb.client.gridfs.model.GridFSFile>();
		GridFSFindIterable result = operations.find(query(whereMetaData("key").is("value")));
		result.into(files);

		assertThat(files.size(), is(1));
		assertEquals(((BsonObjectId) files.get(0).getId()).getValue(), reference);
	}

	@Test // DATAMONGO-6
	public void marshalsComplexMetadata() throws IOException {

		Metadata metadata = new Metadata();
		metadata.version = "1.0";

		ObjectId reference = operations.store(resource.getInputStream(), "foo.xml", metadata);

		List<com.mongodb.client.gridfs.model.GridFSFile> files = new ArrayList<com.mongodb.client.gridfs.model.GridFSFile>();
		GridFSFindIterable result = operations.find(query(whereFilename().is("foo.xml")));
		result.into(files);

		assertThat(files.size(), is(1));
		assertEquals(((BsonObjectId) files.get(0).getId()).getValue(), reference);
	}

	@Test // DATAMONGO-6
	public void findsFilesByResourcePattern() throws IOException {

		ObjectId reference = operations.store(resource.getInputStream(), "foo.xml");

		GridFsResource[] resources = operations.getResources("*.xml");

		assertThat(resources.length, is(1));
		assertThat(((BsonObjectId) resources[0].getId()).getValue(), is(reference));
		assertThat(resources[0].contentLength(), is(resource.contentLength()));
		// assertThat(resources[0].getContentType(), is(resource.()));
	}

	@Test // DATAMONGO-6
	public void findsFilesByResourceLocation() throws IOException {

		ObjectId reference = operations.store(resource.getInputStream(), "foo.xml");

		GridFsResource[] resources = operations.getResources("foo.xml");
		assertThat(resources.length, is(1));
		assertThat(((BsonObjectId) resources[0].getId()).getValue(), is(reference));
		assertThat(resources[0].contentLength(), is(resource.contentLength()));
		// assertThat(resources[0].getContentType(), is(reference.getContentType()));
	}

	@Test // DATAMONGO-503
	public void storesContentType() throws IOException {

		ObjectId reference = operations.store(resource.getInputStream(), "foo2.xml", "application/xml");

		List<com.mongodb.client.gridfs.model.GridFSFile> files = new ArrayList<com.mongodb.client.gridfs.model.GridFSFile>();
		GridFSFindIterable result = operations.find(query(whereContentType().is("application/xml")));
		result.into(files);

		assertThat(files.size(), is(1));
		assertEquals(((BsonObjectId) files.get(0).getId()).getValue(), reference);
	}

	@Test // DATAMONGO-534
	public void considersSortWhenQueryingFiles() throws IOException {

		ObjectId second = operations.store(resource.getInputStream(), "foo.xml");
		ObjectId third = operations.store(resource.getInputStream(), "foobar.xml");
		ObjectId first = operations.store(resource.getInputStream(), "bar.xml");

		Query query = new Query().with(Sort.by(Direction.ASC, "filename"));

		List<com.mongodb.client.gridfs.model.GridFSFile> files = new ArrayList<com.mongodb.client.gridfs.model.GridFSFile>();
		GridFSFindIterable result = operations.find(query);
		result.into(files);

		assertThat(files, hasSize(3));
		assertEquals(((BsonObjectId) files.get(0).getId()).getValue(), first);
		assertEquals(((BsonObjectId) files.get(1).getId()).getValue(), second);
		assertEquals(((BsonObjectId) files.get(2).getId()).getValue(), third);
	}

	@Test // DATAMONGO-534
	public void queryingWithNullQueryReturnsAllFiles() throws IOException {

		ObjectId reference = operations.store(resource.getInputStream(), "foo.xml");

		List<com.mongodb.client.gridfs.model.GridFSFile> files = new ArrayList<com.mongodb.client.gridfs.model.GridFSFile>();
		GridFSFindIterable result = operations.find(null);
		result.into(files);

		assertThat(files, hasSize(1));
		assertEquals(((BsonObjectId) files.get(0).getId()).getValue(), reference);
	}

	@Test // DATAMONGO-813
	public void getResourceShouldReturnNullForNonExistingResource() {
		assertThat(operations.getResource("doesnotexist"), is(nullValue()));
	}

	@Test // DATAMONGO-809
	public void storesAndFindsSimpleDocumentWithMetadataDocument() throws IOException {

		Document metadata = new Document("key", "value");
		ObjectId reference = operations.store(resource.getInputStream(), "foobar", metadata);

		List<com.mongodb.client.gridfs.model.GridFSFile> files = new ArrayList<com.mongodb.client.gridfs.model.GridFSFile>();
		GridFSFindIterable result = operations.find(query(whereMetaData("key").is("value")));
		result.into(files);

		assertThat(files, hasSize(1));
		assertEquals(((BsonObjectId) files.get(0).getId()).getValue(), reference);
	}

	@Test // DATAMONGO-809
	public void storesAndFindsSimpleDocumentWithMetadataObject() throws IOException {

		Metadata metadata = new Metadata();
		metadata.version = "1.0";
		ObjectId reference = operations.store(resource.getInputStream(), "foobar", metadata);

		List<com.mongodb.client.gridfs.model.GridFSFile> files = new ArrayList<com.mongodb.client.gridfs.model.GridFSFile>();
		GridFSFindIterable result = operations.find(query(whereMetaData("version").is("1.0")));
		result.into(files);

		assertThat(files, hasSize(1));
		assertEquals(((BsonObjectId) files.get(0).getId()).getValue(), reference);
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
