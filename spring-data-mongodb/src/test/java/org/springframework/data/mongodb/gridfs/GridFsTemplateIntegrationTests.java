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
package org.springframework.data.mongodb.gridfs;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;
import static org.springframework.data.mongodb.gridfs.GridFsCriteria.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.bson.BsonObjectId;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.SimpleMongoClientDbFactory;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.StreamUtils;

import com.mongodb.MongoGridFSException;
import com.mongodb.client.gridfs.GridFSFindIterable;
import com.mongodb.client.gridfs.model.GridFSFile;

/**
 * Integration tests for {@link GridFsTemplate}.
 *
 * @author Oliver Gierke
 * @author Philipp Schneider
 * @author Thomas Darimont
 * @author Martin Baumgartner
 * @author Hartmut Lang
 * @author Mark Paluch
 * @author Denis Zavedeev
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:gridfs/gridfs.xml")
public class GridFsTemplateIntegrationTests {

	Resource resource = new ClassPathResource("gridfs/gridfs.xml");

	@Autowired GridFsOperations operations;
	@Autowired SimpleMongoClientDbFactory mongoClient;

	@Before
	public void setUp() {
		operations.delete(new Query());
	}

	@Test // DATAMONGO-6
	public void storesAndFindsSimpleDocument() throws IOException {

		ObjectId reference = operations.store(resource.getInputStream(), "foo.xml");

		List<com.mongodb.client.gridfs.model.GridFSFile> files = new ArrayList<>();
		GridFSFindIterable result = operations.find(query(where("_id").is(reference)));
		result.into(files);
		assertThat(files).hasSize(1);
		assertThat(((BsonObjectId) files.get(0).getId()).getValue()).isEqualTo(reference);
	}

	// @Test // DATAMONGO-2392
	// public void storesAndFindsByUUID() throws IOException {
	//
	// UUID uuid = UUID.randomUUID();
	//
	// GridFSFile fs = new GridFSFile(new BsonObjectId(new ObjectId(uuid.to))
	// GridFSInputFile in = fs.createFile(resource.getInputStream(), "gridfs.xml");
	//
	// in.put("_id", uuid);
	// in.put("contentType", "application/octet-stream");
	// in.save();
	//
	// GridFSFile file = operations.findOne(query(where("_id").is(uuid)));
	// GridFsResource resource = operations.getResource(file);
	//
	// assertThat(resource.exists()).isTrue();
	// }

	@Test // DATAMONGO-6
	public void writesMetadataCorrectly() throws IOException {

		Document metadata = new Document("key", "value");
		ObjectId reference = operations.store(resource.getInputStream(), "foo.xml", metadata);

		List<com.mongodb.client.gridfs.model.GridFSFile> files = new ArrayList<>();
		GridFSFindIterable result = operations.find(query(whereMetaData("key").is("value")));
		result.into(files);

		assertThat(files).hasSize(1);
		assertThat(((BsonObjectId) files.get(0).getId()).getValue()).isEqualTo(reference);
	}

	@Test // DATAMONGO-6
	public void marshalsComplexMetadata() throws IOException {

		Metadata metadata = new Metadata();
		metadata.version = "1.0";

		ObjectId reference = operations.store(resource.getInputStream(), "foo.xml", metadata);

		List<com.mongodb.client.gridfs.model.GridFSFile> files = new ArrayList<>();
		GridFSFindIterable result = operations.find(query(whereFilename().is("foo.xml")));
		result.into(files);

		assertThat(files).hasSize(1);
		assertThat(((BsonObjectId) files.get(0).getId()).getValue()).isEqualTo(reference);
	}

	@Test // DATAMONGO-6
	public void findsFilesByResourcePattern() throws IOException {

		ObjectId reference = operations.store(resource.getInputStream(), "foo.xml");

		GridFsResource[] resources = operations.getResources("*.xml");

		assertThat(resources).hasSize(1);
		assertThat(((BsonObjectId) resources[0].getId()).getValue()).isEqualTo(reference);
		assertThat(resources[0].contentLength()).isEqualTo(resource.contentLength());
	}

	@Test // DATAMONGO-6
	public void findsFilesByResourceLocation() throws IOException {

		ObjectId reference = operations.store(resource.getInputStream(), "foo.xml");

		GridFsResource[] resources = operations.getResources("foo.xml");
		assertThat(resources).hasSize(1);
		assertThat(((BsonObjectId) resources[0].getId()).getValue()).isEqualTo(reference);
		assertThat(resources[0].contentLength()).isEqualTo(resource.contentLength());
	}

	@Test // DATAMONGO-503
	public void storesContentType() throws IOException {

		ObjectId reference = operations.store(resource.getInputStream(), "foo2.xml", "application/xml");

		List<com.mongodb.client.gridfs.model.GridFSFile> files = new ArrayList<>();
		GridFSFindIterable result = operations.find(query(whereContentType().is("application/xml")));
		result.into(files);

		assertThat(files).hasSize(1);
		assertThat(((BsonObjectId) files.get(0).getId()).getValue()).isEqualTo(reference);
	}

	@Test // DATAMONGO-534
	public void considersSortWhenQueryingFiles() throws IOException {

		ObjectId second = operations.store(resource.getInputStream(), "foo.xml");
		ObjectId third = operations.store(resource.getInputStream(), "foobar.xml");
		ObjectId first = operations.store(resource.getInputStream(), "bar.xml");

		Query query = new Query().with(Sort.by(Direction.ASC, "filename"));

		List<com.mongodb.client.gridfs.model.GridFSFile> files = new ArrayList<>();
		GridFSFindIterable result = operations.find(query);
		result.into(files);

		assertThat(files).hasSize(3).extracting(it -> ((BsonObjectId) it.getId()).getValue()).containsExactly(first, second,
				third);
	}

	@Test // DATAMONGO-534, DATAMONGO-1762
	public void queryingWithEmptyQueryReturnsAllFiles() throws IOException {

		ObjectId reference = operations.store(resource.getInputStream(), "foo.xml");

		List<com.mongodb.client.gridfs.model.GridFSFile> files = new ArrayList<>();
		GridFSFindIterable result = operations.find(new Query());
		result.into(files);

		assertThat(files).hasSize(1).extracting(it -> ((BsonObjectId) it.getId()).getValue()).containsExactly(reference);
	}

	@Test // DATAMONGO-1762
	public void queryingWithNullQueryThrowsException() {
		assertThatIllegalArgumentException().isThrownBy(() -> operations.find(null));
	}

	@Test // DATAMONGO-813, DATAMONGO-1914
	public void getResourceShouldReturnAbsentResourceForNonExistingResource() {
		assertThat(operations.getResource("doesnotexist")).isEqualTo(GridFsResource.absent("doesnotexist"));
	}

	@Test // DATAMONGO-809
	public void storesAndFindsSimpleDocumentWithMetadataDocument() throws IOException {

		Document metadata = new Document("key", "value");
		ObjectId reference = operations.store(resource.getInputStream(), "foobar", metadata);

		List<com.mongodb.client.gridfs.model.GridFSFile> files = new ArrayList<>();
		GridFSFindIterable result = operations.find(query(whereMetaData("key").is("value")));
		result.into(files);

		assertThat(files).hasSize(1).extracting(it -> ((BsonObjectId) it.getId()).getValue()).containsExactly(reference);
	}

	@Test // DATAMONGO-809
	public void storesAndFindsSimpleDocumentWithMetadataObject() throws IOException {

		Metadata metadata = new Metadata();
		metadata.version = "1.0";
		ObjectId reference = operations.store(resource.getInputStream(), "foobar", metadata);

		List<com.mongodb.client.gridfs.model.GridFSFile> files = new ArrayList<>();
		GridFSFindIterable result = operations.find(query(whereMetaData("version").is("1.0")));
		result.into(files);

		assertThat(files).hasSize(1).extracting(it -> ((BsonObjectId) it.getId()).getValue()).containsExactly(reference);
	}

	@Test // DATAMONGO-1695
	public void readsContentTypeCorrectly() throws IOException {

		operations.store(resource.getInputStream(), "someName", "contentType");

		assertThat(operations.getResource("someName").getContentType()).isEqualTo("contentType");
	}

	@Test // DATAMONGO-1850
	public void failsOnNonExistingContentTypeRetrieval() throws IOException {

		operations.store(resource.getInputStream(), "no-content-type", (String) null);
		GridFsResource result = operations.getResource("no-content-type");

		assertThatThrownBy(result::getContentType).isInstanceOf(MongoGridFSException.class);
	}

	@Test // DATAMONGO-1813
	public void convertFileToResource() throws IOException {

		Document metadata = new Document("key", "value");
		ObjectId reference = operations.store(resource.getInputStream(), "foobar", metadata);

		GridFSFile file = operations.findOne(query(whereMetaData("key").is("value")));
		GridFsResource result = operations.getResource(file);

		assertThat(result.contentLength()).isEqualTo(resource.contentLength());
		assertThat(((BsonObjectId) result.getId()).getValue()).isEqualTo(reference);
	}

	@Test // DATAMONGO-2021
	public void getResourceShouldRetrieveContentByIdentity() throws IOException {

		ClassPathResource secondResource = new ClassPathResource("gridfs/another-resource.xml");

		ObjectId reference1 = operations.store(resource.getInputStream(), "foo.xml");
		ObjectId reference2 = operations.store(secondResource.getInputStream(), "foo.xml");

		Map<ObjectId, Resource> fixture = new LinkedHashMap<>();
		fixture.put(reference1, resource);
		fixture.put(reference2, secondResource);

		for (Entry<ObjectId, Resource> entry : fixture.entrySet()) {

			GridFsResource fsFile = operations.getResource(operations.findOne(query(where("_id").is(entry.getKey()))));
			byte[] content = StreamUtils.copyToByteArray(fsFile.getInputStream());

			assertThat(content).isEqualTo(StreamUtils.copyToByteArray(entry.getValue().getInputStream()));
		}
	}

	@Test // DATAMONGO-765
	public void considersSkipLimitWhenQueryingFiles() {

		Stream.of("a", "aa", "aaa", //
				"b", "bb", "bb", //
				"c", "cc", "ccc", //
				"d", "dd", "ddd") //
				.forEach(filename -> operations.store(new ByteArrayInputStream(new byte[0]), filename));

		PageRequest pageRequest = PageRequest.of(2, 3, Direction.ASC, "filename");
		List<String> filenames = operations.find(new Query().with(pageRequest)) //
				.map(GridFSFile::getFilename) //
				.into(new ArrayList<>());

		assertThat(filenames).containsExactly("c", "cc", "ccc");
	}

	class Metadata {
		String version;
	}
}
