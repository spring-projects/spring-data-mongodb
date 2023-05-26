/*
 * Copyright 2019-2023 the original author or authors.
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

import java.nio.ByteBuffer;

import org.bson.BsonValue;
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
import org.springframework.data.mongodb.util.BsonUtils;
import org.springframework.data.util.Lazy;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.reactivestreams.client.gridfs.GridFSBucket;
import com.mongodb.reactivestreams.client.gridfs.GridFSBuckets;
import com.mongodb.reactivestreams.client.gridfs.GridFSFindPublisher;
import com.mongodb.reactivestreams.client.gridfs.GridFSUploadPublisher;

/**
 * {@link ReactiveGridFsOperations} implementation to store content into MongoDB GridFS. Uses by default
 * {@link DefaultDataBufferFactory} to create {@link DataBuffer buffers}.
 *
 * @author Mark Paluch
 * @author Nick Stolwijk
 * @author Denis Zavedeev
 * @author Christoph Strobl
 * @author Mathieu Ouellet
 * @since 2.2
 */
public class ReactiveGridFsTemplate extends GridFsOperationsSupport implements ReactiveGridFsOperations {

	private final DataBufferFactory dataBufferFactory;
	private final Mono<GridFSBucket> bucketSupplier;

	/**
	 * Creates a new {@link ReactiveGridFsTemplate} using the given {@link ReactiveMongoDatabaseFactory} and
	 * {@link MongoConverter}.
	 * <p>
	 * Note that the {@link GridFSBucket} is obtained only once from
	 * {@link ReactiveMongoDatabaseFactory#getMongoDatabase() MongoDatabase}. Use
	 * {@link #ReactiveGridFsTemplate(MongoConverter, Mono, DataBufferFactory)} if you want to use different buckets from
	 * the same Template instance.
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
	 * <p>
	 * Note that the {@link GridFSBucket} is obtained only once from
	 * {@link ReactiveMongoDatabaseFactory#getMongoDatabase() MongoDatabase}. Use
	 * {@link #ReactiveGridFsTemplate(MongoConverter, Mono, DataBufferFactory)} if you want to use different buckets from
	 * the same Template instance.
	 *
	 * @param dbFactory must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @param bucket can be {@literal null}.
	 */
	public ReactiveGridFsTemplate(ReactiveMongoDatabaseFactory dbFactory, MongoConverter converter,
			@Nullable String bucket) {
		this(new DefaultDataBufferFactory(), dbFactory, converter, bucket);
	}

	/**
	 * Creates a new {@link ReactiveGridFsTemplate} using the given {@link DataBufferFactory},
	 * {@link ReactiveMongoDatabaseFactory} and {@link MongoConverter}.
	 * <p>
	 * Note that the {@link GridFSBucket} is obtained only once from
	 * {@link ReactiveMongoDatabaseFactory#getMongoDatabase() MongoDatabase}. Use
	 * {@link #ReactiveGridFsTemplate(MongoConverter, Mono, DataBufferFactory)} if you want to use different buckets from
	 * the same Template instance.
	 *
	 * @param dataBufferFactory must not be {@literal null}.
	 * @param dbFactory must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @param bucket can be {@literal null}.
	 */
	public ReactiveGridFsTemplate(DataBufferFactory dataBufferFactory, ReactiveMongoDatabaseFactory dbFactory,
			MongoConverter converter, @Nullable String bucket) {
		this(converter, Mono.defer(Lazy.of(() -> doGetBucket(dbFactory, bucket))), dataBufferFactory);
	}

	/**
	 * Creates a new {@link ReactiveGridFsTemplate} using the given {@link MongoConverter}, {@link Mono} emitting a
	 * {@link ReactiveMongoDatabaseFactory} and {@link DataBufferFactory}.
	 *
	 * @param converter must not be {@literal null}.
	 * @param gridFSBucket must not be {@literal null}.
	 * @param dataBufferFactory must not be {@literal null}.
	 * @since 4.2
	 */
	public ReactiveGridFsTemplate(MongoConverter converter, Mono<GridFSBucket> gridFSBucket,
			DataBufferFactory dataBufferFactory) {

		super(converter);

		Assert.notNull(gridFSBucket, "GridFSBucket Mono must not be null");
		Assert.notNull(dataBufferFactory, "DataBufferFactory must not be null");

		this.bucketSupplier = gridFSBucket;
		this.dataBufferFactory = dataBufferFactory;
	}

	@Override
	public Mono<ObjectId> store(Publisher<DataBuffer> content, @Nullable String filename, @Nullable String contentType,
			@Nullable Object metadata) {
		return store(content, filename, contentType, toDocument(metadata));
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> Mono<T> store(GridFsObject<T, Publisher<DataBuffer>> upload) {

		GridFSUploadOptions uploadOptions = computeUploadOptionsFor(upload.getOptions().getContentType(),
				upload.getOptions().getMetadata());

		if (upload.getOptions().getChunkSize() > 0) {
			uploadOptions.chunkSizeBytes(upload.getOptions().getChunkSize());
		}

		String filename = upload.getFilename();
		Flux<ByteBuffer> source = Flux.from(upload.getContent()).map(DataBuffer::toByteBuffer);
		T fileId = upload.getFileId();

		if (fileId == null) {
			return (Mono<T>) createMono(new AutoIdCreatingUploadCallback(filename, source, uploadOptions));
		}

		UploadCallback callback = new UploadCallback(BsonUtils.simpleToBsonValue(fileId), filename, source, uploadOptions);
		return createMono(callback).thenReturn(fileId);
	}

	@Override
	public Flux<GridFSFile> find(Query query) {

		Document queryObject = getMappedQuery(query.getQueryObject());
		Document sortObject = getMappedQuery(query.getSortObject());

		return createFlux(new FindCallback(query, queryObject, sortObject));
	}

	@Override
	public Mono<GridFSFile> findOne(Query query) {

		Document queryObject = getMappedQuery(query.getQueryObject());
		Document sortObject = getMappedQuery(query.getSortObject());

		return createFlux(new FindLimitCallback(query, queryObject, sortObject, 2)) //
				.collectList() //
				.handle((files, sink) -> {

					if (files.size() == 1) {
						sink.next(files.get(0));
						return;
					}

					if (files.size() > 1) {
						sink.error(new IncorrectResultSizeDataAccessException(
								"Query " + SerializationUtils.serializeToJsonSafely(query) + " returned non unique result.", 1));
					}
				});
	}

	@Override
	public Mono<GridFSFile> findFirst(Query query) {

		Document queryObject = getMappedQuery(query.getQueryObject());
		Document sortObject = getMappedQuery(query.getSortObject());

		return createFlux(new FindLimitCallback(query, queryObject, sortObject, 1)).next();
	}

	@Override
	public Mono<Void> delete(Query query) {
		return find(query).flatMap(it -> createMono(new DeleteCallback(it.getId()))).then();
	}

	@Override
	public Mono<ReactiveGridFsResource> getResource(String location) {

		Assert.notNull(location, "Filename must not be null");

		return findOne(query(whereFilename().is(location))).flatMap(this::getResource)
				.defaultIfEmpty(ReactiveGridFsResource.absent(location));
	}

	@Override
	public Mono<ReactiveGridFsResource> getResource(GridFSFile file) {

		Assert.notNull(file, "GridFSFile must not be null");

		return doGetBucket()
				.map(it -> new ReactiveGridFsResource(file, it.downloadToPublisher(file.getId()), dataBufferFactory));
	}

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

	/**
	 * Create a reusable Mono for a {@link ReactiveBucketCallback}. It's up to the developer to choose to obtain a new
	 * {@link Flux} or to reuse the {@link Flux}.
	 *
	 * @param callback must not be {@literal null}
	 * @return a {@link Mono} wrapping the {@link ReactiveBucketCallback}.
	 */
	public <T> Mono<T> createMono(ReactiveBucketCallback<T> callback) {

		Assert.notNull(callback, "ReactiveBucketCallback must not be null");

		return doGetBucket().flatMap(bucket -> Mono.from(callback.doInBucket(bucket)));
	}

	/**
	 * Create a reusable Flux for a {@link ReactiveBucketCallback}. It's up to the developer to choose to obtain a new
	 * {@link Flux} or to reuse the {@link Flux}.
	 *
	 * @param callback must not be {@literal null}
	 * @return a {@link Flux} wrapping the {@link ReactiveBucketCallback}.
	 */
	public <T> Flux<T> createFlux(ReactiveBucketCallback<T> callback) {

		Assert.notNull(callback, "ReactiveBucketCallback must not be null");

		return doGetBucket().flatMapMany(callback::doInBucket);
	}

	protected Mono<GridFSBucket> doGetBucket() {
		return bucketSupplier;
	}

	private static Mono<GridFSBucket> doGetBucket(ReactiveMongoDatabaseFactory dbFactory, @Nullable String bucket) {

		Assert.notNull(dbFactory, "ReactiveMongoDatabaseFactory must not be null");

		return dbFactory.getMongoDatabase()
				.map(db -> bucket == null ? GridFSBuckets.create(db) : GridFSBuckets.create(db, bucket));
	}

	/**
	 * @param <T>
	 * @author Mathieu Ouellet
	 * @since 3.0
	 */
	interface ReactiveBucketCallback<T> {
		Publisher<T> doInBucket(GridFSBucket bucket);
	}

	private static class FindCallback implements ReactiveBucketCallback<GridFSFile> {

		private final Query query;
		private final Document queryObject;
		private final Document sortObject;

		public FindCallback(Query query, Document queryObject, Document sortObject) {

			this.query = query;
			this.queryObject = queryObject;
			this.sortObject = sortObject;
		}

		@Override
		public GridFSFindPublisher doInBucket(GridFSBucket bucket) {

			GridFSFindPublisher findPublisher = bucket.find(queryObject).sort(sortObject);

			if (query.getLimit() > 0) {
				findPublisher = findPublisher.limit(query.getLimit());
			}

			if (query.getSkip() > 0) {
				findPublisher = findPublisher.skip(Math.toIntExact(query.getSkip()));
			}

			Integer cursorBatchSize = query.getMeta().getCursorBatchSize();
			if (cursorBatchSize != null) {
				findPublisher = findPublisher.batchSize(cursorBatchSize);
			}

			return findPublisher;
		}
	}

	private static class FindLimitCallback extends FindCallback {

		private final int limit;

		public FindLimitCallback(Query query, Document queryObject, Document sortObject, int limit) {

			super(query, queryObject, sortObject);
			this.limit = limit;
		}

		@Override
		public GridFSFindPublisher doInBucket(GridFSBucket bucket) {
			return super.doInBucket(bucket).limit(limit);
		}
	}

	private record UploadCallback(BsonValue fileId, String filename, Publisher<ByteBuffer> source,
			GridFSUploadOptions uploadOptions) implements ReactiveBucketCallback<Void> {

		@Override
		public GridFSUploadPublisher<Void> doInBucket(GridFSBucket bucket) {
			return bucket.uploadFromPublisher(fileId, filename, source, uploadOptions);
		}
	}

	private record AutoIdCreatingUploadCallback(String filename, Publisher<ByteBuffer> source,
			GridFSUploadOptions uploadOptions) implements ReactiveBucketCallback<ObjectId> {

		@Override
		public GridFSUploadPublisher<ObjectId> doInBucket(GridFSBucket bucket) {
			return bucket.uploadFromPublisher(filename, source, uploadOptions);
		}
	}

	private record DeleteCallback(BsonValue id) implements ReactiveBucketCallback<Void> {

		@Override
		public Publisher<Void> doInBucket(GridFSBucket bucket) {
			return bucket.delete(id);
		}
	}

}
