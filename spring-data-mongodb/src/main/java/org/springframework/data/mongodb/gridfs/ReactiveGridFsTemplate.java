/*
 * Copyright 2019 the original author or authors.
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

import static org.springframework.data.mongodb.core.query.Query.*;
import static org.springframework.data.mongodb.gridfs.GridFsCriteria.*;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.reactivestreams.Publisher;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.SerializationUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.mongodb.client.gridfs.model.GridFSFile;
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
public class ReactiveGridFsTemplate extends GridFsOperationsSupport implements ReactiveGridFsOperations {

	private final ReactiveMongoDatabaseFactory dbFactory;
	private final DataBufferFactory dataBufferFactory;
	private final @Nullable String bucket;

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

		super(converter);

		Assert.notNull(dataBufferFactory, "DataBufferFactory must not be null!");
		Assert.notNull(dbFactory, "ReactiveMongoDatabaseFactory must not be null!");

		this.dataBufferFactory = dataBufferFactory;
		this.dbFactory = dbFactory;
		this.bucket = bucket;
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
		return Mono.from(getGridFs().uploadFromStream(filename, content, computeUploadOptionsFor(contentType, metadata)));
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
		return Flux.from(prepareQuery(query));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.gridfs.ReactiveGridFsOperations#findOne(org.springframework.data.mongodb.core.query.Query)
	 */
	@Override
	public Mono<GridFSFile> findOne(Query query) {

		return Flux.from(prepareQuery(query).limit(2)) //
				.collectList() //
				.flatMap(it -> {
					if (it.isEmpty()) {
						return Mono.empty();
					}

					if (it.size() > 1) {
						return Mono.error(new IncorrectResultSizeDataAccessException(
								"Query " + SerializationUtils.serializeToJsonSafely(query) + " returned non unique result.", 1));
					}

					return Mono.just(it.get(0));
				});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.gridfs.ReactiveGridFsOperations#findFirst(org.springframework.data.mongodb.core.query.Query)
	 */
	@Override
	public Mono<GridFSFile> findFirst(Query query) {
		return Flux.from(prepareQuery(query).limit(1)).next();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.gridfs.ReactiveGridFsOperations#delete(org.springframework.data.mongodb.core.query.Query)
	 */
	@Override
	public Mono<Void> delete(Query query) {
		return find(query).flatMap(it -> getGridFs().delete(it.getId())).then();
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

	protected GridFSBucket getGridFs() {

		MongoDatabase db = dbFactory.getMongoDatabase();
		return bucket == null ? GridFSBuckets.create(db) : GridFSBuckets.create(db, bucket);
	}
}
