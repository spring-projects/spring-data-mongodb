/*
 * Copyright 2019 the original author or authors.
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

import static org.springframework.data.mongodb.core.query.Query.*;
import static org.springframework.data.mongodb.gridfs.GridFsCriteria.*;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Optional;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.reactivestreams.Publisher;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.mongodb.reactivestreams.client.gridfs.AsyncInputStream;
import com.mongodb.reactivestreams.client.gridfs.GridFSBucket;
import com.mongodb.reactivestreams.client.gridfs.GridFSBuckets;
import com.mongodb.reactivestreams.client.gridfs.GridFSDownloadStream;
import com.mongodb.reactivestreams.client.gridfs.GridFSFindPublisher;

/**
 * {@link ReactiveGridFsOperations} implementation to store content into MongoDB GridFS. Uses by default
 * {@link DefaultDataBufferFactory} to create {@link DataBuffer buffers}.
 *
 * @author Mark Paluch
 * @since 2.2
 */
public class ReactiveGridFsTemplate implements ReactiveGridFsOperations {

	private final DataBufferFactory dataBufferFactory;
	private final ReactiveMongoDatabaseFactory dbFactory;
	private final @Nullable String bucket;
	private final MongoConverter converter;
	private final QueryMapper queryMapper;

	/**
	 * Creates a new {@link ReactiveGridFsTemplate} using the given {@link ReactiveMongoDatabaseFactory} and
	 * {@link MongoConverter}.
	 *
	 * @param dbFactory must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 */
	public ReactiveGridFsTemplate(ReactiveMongoDatabaseFactory dbFactory, MongoConverter converter) {
		this(dbFactory, converter, null);
	}

	/**
	 * Creates a new {@link ReactiveGridFsTemplate} using the given {@link ReactiveMongoDatabaseFactory} and
	 * {@link MongoConverter}.
	 *
	 * @param dbFactory must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @param bucket
	 */
	public ReactiveGridFsTemplate(ReactiveMongoDatabaseFactory dbFactory, MongoConverter converter,
			@Nullable String bucket) {
		this(new DefaultDataBufferFactory(), dbFactory, converter, bucket);
	}

	/**
	 * Creates a new {@link ReactiveGridFsTemplate} using the given {@link DataBufferFactory},
	 * {@link ReactiveMongoDatabaseFactory} and {@link MongoConverter}.
	 *
	 * @param dataBufferFactory must not be {@literal null}.
	 * @param dbFactory must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @param bucket
	 */
	public ReactiveGridFsTemplate(DataBufferFactory dataBufferFactory, ReactiveMongoDatabaseFactory dbFactory,
			MongoConverter converter, @Nullable String bucket) {

		Assert.notNull(dataBufferFactory, "DataBufferFactory must not be null!");
		Assert.notNull(dbFactory, "ReactiveMongoDatabaseFactory must not be null!");
		Assert.notNull(converter, "MongoConverter must not be null!");

		this.dataBufferFactory = dataBufferFactory;
		this.dbFactory = dbFactory;
		this.converter = converter;
		this.bucket = bucket;

		this.queryMapper = new QueryMapper(converter);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.gridfs.ReactiveGridFsOperations#store(com.mongodb.reactivestreams.client.gridfs.AsyncInputStream, java.lang.String, java.lang.String, java.lang.Object)
	 */
	@Override
	public Mono<ObjectId> store(AsyncInputStream content, @Nullable String filename, @Nullable String contentType,
			@Nullable Object metadata) {
		return store(content, filename, contentType, toDocument(metadata));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.gridfs.ReactiveGridFsOperations#store(org.reactivestreams.Publisher, java.lang.String, java.lang.String, java.lang.Object)
	 */
	@Override
	public Mono<ObjectId> store(Publisher<DataBuffer> content, @Nullable String filename, @Nullable String contentType,
			@Nullable Object metadata) {
		return store(content, filename, contentType, toDocument(metadata));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.gridfs.ReactiveGridFsOperations#store(com.mongodb.reactivestreams.client.gridfs.AsyncInputStream, java.lang.String, java.lang.String, org.bson.Document)
	 */
	@Override
	public Mono<ObjectId> store(AsyncInputStream content, @Nullable String filename, @Nullable String contentType,
			@Nullable Document metadata) {

		Assert.notNull(content, "InputStream must not be null!");

		GridFSUploadOptions options = new GridFSUploadOptions();

		Document mData = new Document();

		if (StringUtils.hasText(contentType)) {
			mData.put(GridFsResource.CONTENT_TYPE_FIELD, contentType);
		}

		if (metadata != null) {
			mData.putAll(metadata);
		}

		options.metadata(mData);

		return Mono.from(getGridFs().uploadFromStream(filename, content, options));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.gridfs.ReactiveGridFsOperations#store(org.reactivestreams.Publisher, java.lang.String, java.lang.String, org.bson.Document)
	 */
	@Override
	public Mono<ObjectId> store(Publisher<DataBuffer> content, @Nullable String filename, @Nullable String contentType,
			@Nullable Document metadata) {

		Assert.notNull(content, "Content must not be null!");

		return BinaryStreamAdapters.toAsyncInputStream(content).flatMap(it -> store(it, filename, contentType, metadata));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.gridfs.ReactiveGridFsOperations#find(org.springframework.data.mongodb.core.query.Query)
	 */
	@Override
	public Flux<GridFSFile> find(Query query) {

		GridFSFindPublisher publisherToUse = prepareQuery(query);

		return Flux.from(publisherToUse);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.gridfs.ReactiveGridFsOperations#findOne(org.springframework.data.mongodb.core.query.Query)
	 */
	@Override
	public Mono<GridFSFile> findOne(Query query) {

		GridFSFindPublisher publisherToUse = prepareQuery(query);

		return Flux.from(publisherToUse.limit(1)).next();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.gridfs.ReactiveGridFsOperations#delete(org.springframework.data.mongodb.core.query.Query)
	 */
	@Override
	public Mono<Void> delete(Query query) {

		GridFSBucket gridFs = getGridFs();
		return find(query).flatMap(it -> gridFs.delete(it.getId())).then();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.gridfs.ReactiveGridFsOperations#getResource(java.lang.String)
	 */
	@Override
	public Mono<ReactiveGridFsResource> getResource(String location) {

		Assert.notNull(location, "Filename must not be null!");

		return findOne(query(whereFilename().is(location))).flatMap(this::getResource)
				.defaultIfEmpty(ReactiveGridFsResource.absent(location));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.gridfs.ReactiveGridFsOperations#getResource(com.mongodb.client.gridfs.model.GridFSFile)
	 */
	@Override
	public Mono<ReactiveGridFsResource> getResource(GridFSFile file) {

		Assert.notNull(file, "GridFSFile must not be null!");

		return Mono.fromSupplier(() -> {

			GridFSDownloadStream stream = getGridFs().openDownloadStream(file.getObjectId());

			return new ReactiveGridFsResource(file, BinaryStreamAdapters.toPublisher(stream, dataBufferFactory));
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.gridfs.ReactiveGridFsOperations#getResources(java.lang.String)
	 */
	@Override
	public Flux<ReactiveGridFsResource> getResources(String locationPattern) {

		if (!StringUtils.hasText(locationPattern)) {
			return Flux.empty();
		}

		AntPath path = new AntPath(locationPattern);

		if (path.isPattern()) {

			Flux<GridFSFile> files = find(query(whereFilename().regex(path.toRegex())));
			return files.flatMap(this::getResource);
		}

		return getResource(locationPattern).flux();
	}

	protected GridFSFindPublisher prepareQuery(Query query) {

		Assert.notNull(query, "Query must not be null!");

		Document queryObject = getMappedQuery(query.getQueryObject());
		Document sortObject = getMappedQuery(query.getSortObject());

		GridFSFindPublisher publisherToUse = getGridFs().find(queryObject).sort(sortObject);

		Integer cursorBatchSize = query.getMeta().getCursorBatchSize();
		if (cursorBatchSize != null) {
			publisherToUse = publisherToUse.batchSize(cursorBatchSize);
		}

		return publisherToUse;
	}

	private Document getMappedQuery(Document query) {
		return queryMapper.getMappedObject(query, Optional.empty());
	}

	protected GridFSBucket getGridFs() {

		MongoDatabase db = dbFactory.getMongoDatabase();
		return bucket == null ? GridFSBuckets.create(db) : GridFSBuckets.create(db, bucket);
	}

	@Nullable
	private Document toDocument(@Nullable Object metadata) {

		Document document = null;

		if (metadata != null) {
			document = new Document();
			converter.write(metadata, document);
		}
		return document;
	}
}
