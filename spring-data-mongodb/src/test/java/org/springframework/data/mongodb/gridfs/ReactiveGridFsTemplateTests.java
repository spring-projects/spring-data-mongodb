/*
 * Copyright 2019-2024 the original author or authors.
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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;

import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.core.io.buffer.DefaultDataBuffer;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.StreamUtils;

import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.internal.HexUtils;

/**
 * Integration tests for {@link ReactiveGridFsTemplate}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Nick Stolwijk
 * @author Denis Zavedeev
 */
@RunWith(SpringRunner.class)
@ContextConfiguration("classpath:gridfs/reactive-gridfs.xml")
public class ReactiveGridFsTemplateTests {

	Resource resource = new ClassPathResource("gridfs/gridfs.xml");

	@Autowired ReactiveGridFsOperations operations;
	@Autowired SimpleMongoClientDatabaseFactory mongoClient;
	@Autowired ReactiveMongoDatabaseFactory dbFactory;
	@Autowired MongoConverter mongoConverter;

	@Before
	public void setUp() {

		operations.delete(new Query()) //
				.as(StepVerifier::create) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1855
	public void storesAndFindsSimpleDocument() {

		DefaultDataBufferFactory factory = new DefaultDataBufferFactory();
		DefaultDataBuffer first = factory.wrap("first".getBytes());
		DefaultDataBuffer second = factory.wrap("second".getBytes());

		ObjectId reference = operations.store(Flux.just(first, second), "foo.xml").block();

		operations.find(query(where("_id").is(reference))) //
				.as(StepVerifier::create) //
				.assertNext(actual -> {
					assertThat(((BsonObjectId) actual.getId()).getValue()).isEqualTo(reference);
				}) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1855
	public void storesAndLoadsLargeFileCorrectly() {

		ByteBuffer buffer = ByteBuffer.allocate(1000 * 1000); // 1 mb
		int i = 0;
		while (buffer.remaining() != 0) {
			buffer.put(HexUtils.toHex(new byte[] { (byte) (i++ % 16) }).getBytes());
		}
		buffer.flip();

		DefaultDataBufferFactory factory = new DefaultDataBufferFactory();

		ObjectId reference = operations.store(Flux.just(factory.wrap(buffer)), "large.txt").block();

		buffer.clear();

		// default chunk size
		operations.findOne(query(where("_id").is(reference))).flatMap(operations::getResource)
				.flatMapMany(ReactiveGridFsResource::getDownloadStream) //
				.transform(DataBufferUtils::join) //
				.as(StepVerifier::create) //
				.consumeNextWith(dataBuffer -> {

					assertThat(dataBuffer.readableByteCount()).isEqualTo(buffer.remaining());
					assertThat(dataBuffer.asByteBuffer()).isEqualTo(buffer);
				}).verifyComplete();

		// small chunk size
		operations.findOne(query(where("_id").is(reference))).flatMap(operations::getResource)
				.flatMapMany(reactiveGridFsResource -> reactiveGridFsResource.getDownloadStream(256)) //
				.transform(DataBufferUtils::join) //
				.as(StepVerifier::create) //
				.consumeNextWith(dataBuffer -> {

					assertThat(dataBuffer.readableByteCount()).isEqualTo(buffer.remaining());
					assertThat(dataBuffer.asByteBuffer()).isEqualTo(buffer);
				}).verifyComplete();
	}

	@Test // DATAMONGO-1855
	public void writesMetadataCorrectly() throws IOException {

		Document metadata = new Document("key", "value");

		Flux<DataBuffer> source = DataBufferUtils.read(resource, new DefaultDataBufferFactory(), 256);
		ObjectId reference = operations.store(source, "foo.xml", "binary/octet-stream", metadata).block();

		operations.find(query(whereMetaData("key").is("value"))) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual.getObjectId()).isEqualTo(reference);
				}) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1855
	public void marshalsComplexMetadata() {

		Metadata metadata = new Metadata();
		metadata.version = "1.0";

		Flux<DataBuffer> source = DataBufferUtils.read(resource, new DefaultDataBufferFactory(), 256);
		ObjectId reference = operations.store(source, "foo.xml", "binary/octet-stream", metadata).block();

		operations.find(query(whereMetaData("version").is("1.0"))) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual.getObjectId()).isEqualTo(reference);
					assertThat(actual.getMetadata()).containsEntry("version", "1.0");
				}) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1855
	public void getResourceShouldRetrieveContentByIdentity() throws IOException {

		byte[] content = StreamUtils.copyToByteArray(resource.getInputStream());
		Flux<DataBuffer> source = DataBufferUtils.read(resource, new DefaultDataBufferFactory(), 256);
		ObjectId reference = operations.store(source, "foo.xml", null, null).block();

		operations.findOne(query(where("_id").is(reference))).flatMap(operations::getResource)
				.flatMapMany(ReactiveGridFsResource::getDownloadStream) //
				.transform(DataBufferUtils::join) //
				.as(StepVerifier::create) //
				.consumeNextWith(dataBuffer -> {

					byte[] actual = new byte[dataBuffer.readableByteCount()];
					dataBuffer.read(actual);

					assertThat(actual).isEqualTo(content);
				}) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1855, DATAMONGO-2240
	public void shouldEmitFirstEntryWhenFindFirstRetrievesMoreThanOneResult() throws IOException {

		Flux<DataBuffer> upload1 = DataBufferUtils.read(resource, new DefaultDataBufferFactory(), 256);
		Flux<DataBuffer> upload2 = DataBufferUtils.read(new ClassPathResource("gridfs/another-resource.xml"),
				new DefaultDataBufferFactory(), 256);

		operations.store(upload1, "foo.xml", null, null).block();
		operations.store(upload2, "foo2.xml", null, null).block();

		operations.findFirst(query(where("filename").regex("foo*"))) //
				.flatMap(operations::getResource) //
				.as(StepVerifier::create) //
				.assertNext(actual -> {

					assertThat(actual.getGridFSFile()).isNotNull();
				}).verifyComplete();
	}

	@Test // DATAMONGO-2240
	public void shouldReturnNoGridFsFileWhenAbsent() {

		operations.getResource("absent") //
				.as(StepVerifier::create) //
				.assertNext(actual -> {

					assertThat(actual.exists()).isFalse();
					assertThat(actual.getGridFSFile()).isEqualTo(Mono.empty());
				}).verifyComplete();
	}

	@Test // DATAMONGO-1855
	public void shouldEmitErrorWhenFindOneRetrievesMoreThanOneResult() throws IOException {

		Flux<DataBuffer> upload1 = DataBufferUtils.read(resource, new DefaultDataBufferFactory(), 256);
		Flux<DataBuffer> upload2 = DataBufferUtils.read(new ClassPathResource("gridfs/another-resource.xml"),
				new DefaultDataBufferFactory(), 256);

		operations.store(upload1, "foo.xml", null, null).block();
		operations.store(upload2, "foo2.xml", null, null).block();

		operations.findOne(query(where("filename").regex("foo*"))) //
				.as(StepVerifier::create) //
				.expectError(IncorrectResultSizeDataAccessException.class) //
				.verify();
	}

	@Test // DATAMONGO-1855
	public void getResourcesByPattern() throws IOException {

		byte[] content = StreamUtils.copyToByteArray(resource.getInputStream());
		Flux<DataBuffer> upload = DataBufferUtils.read(resource, new DefaultDataBufferFactory(), 256);

		operations.store(upload, "foo.xml", null, null).block();

		operations.getResources("foo*") //
				.flatMap(ReactiveGridFsResource::getDownloadStream) //
				.transform(DataBufferUtils::join) //
				.as(StepVerifier::create) //
				.consumeNextWith(dataBuffer -> {

					byte[] actual = new byte[dataBuffer.readableByteCount()];
					dataBuffer.read(actual);

					assertThat(actual).isEqualTo(content);
				}) //
				.verifyComplete();
	}

	@Test // DATAMONGO-625
	public void storeSavesGridFsUploadWithGivenIdCorrectly() throws IOException {

		String id = "id-1";
		byte[] content = StreamUtils.copyToByteArray(resource.getInputStream());
		Flux<DataBuffer> data = DataBufferUtils.read(resource, new DefaultDataBufferFactory(), 256);

		ReactiveGridFsUpload<String> upload = ReactiveGridFsUpload.fromPublisher(data) //
				.id(id) //
				.filename("gridFsUpload.xml") //
				.contentType("xml") //
				.build();

		operations.store(upload).as(StepVerifier::create).expectNext(id).verifyComplete();

		operations.findOne(query(where("_id").is(id))).flatMap(operations::getResource)
				.flatMapMany(ReactiveGridFsResource::getDownloadStream) //
				.transform(DataBufferUtils::join) //
				.as(StepVerifier::create) //
				.consumeNextWith(dataBuffer -> {

					byte[] actual = new byte[dataBuffer.readableByteCount()];
					dataBuffer.read(actual);

					assertThat(actual).isEqualTo(content);
				}) //
				.verifyComplete();

		operations.findOne(query(where("_id").is(id))).as(StepVerifier::create).consumeNextWith(it -> {
			assertThat(it.getFilename()).isEqualTo("gridFsUpload.xml");
			assertThat(it.getId()).isEqualTo(new BsonString(id));
			assertThat(it.getMetadata()).containsValue("xml");
		}).verifyComplete();
	}

	@Test // DATAMONGO-765
	public void considersSkipLimitWhenQueryingFiles() {

		DataBufferFactory bufferFactory = new DefaultDataBufferFactory();
		DataBuffer buffer = bufferFactory.allocateBuffer(0);
		Flux.just("a", "aa", "aaa", //
				"b", "bb", "bbb", //
				"c", "cc", "ccc", //
				"d", "dd", "ddd") //
				.flatMap(fileName -> operations.store(Mono.just(buffer), fileName)) //
				.as(StepVerifier::create) //
				.expectNextCount(12) //
				.verifyComplete();

		PageRequest pageRequest = PageRequest.of(2, 3, Sort.Direction.ASC, "filename");
		operations.find(new Query().with(pageRequest)) //
				.map(GridFSFile::getFilename) //
				.as(StepVerifier::create) //
				.expectNext("c", "cc", "ccc") //
				.verifyComplete();
	}

	static class Metadata {
		String version;
	}

	public static String readToString(DataBuffer dataBuffer) {
		try {
			return FileCopyUtils.copyToString(new InputStreamReader(dataBuffer.asInputStream()));
		} catch (IOException e) {
			return e.getMessage();
		}
	}
}
