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

import lombok.RequiredArgsConstructor;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DataBufferUtils;
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
import com.mongodb.reactivestreams.client.Success;
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

		return Mono.<Mono<ObjectId>> create(sink -> {

			BinaryPublisherToAsyncInputStreamAdapter adapter = new BinaryPublisherToAsyncInputStreamAdapter(content,
					sink.currentContext());

			Mono<ObjectId> store = store(adapter, filename, contentType, metadata);

			sink.success(store);
		}).flatMap(Function.identity());
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

			return new ReactiveGridFsResource(file, BinaryStreamUtility.createBinaryStream(dataBufferFactory, stream));
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

	/**
	 * Adapter accepting a binary stream {@link Publisher} and transforming its contents into a {@link AsyncInputStream}.
	 * <p>
	 * This adapter subscribes to the binary {@link Publisher} as soon as the first chunk gets {@link #read(ByteBuffer)
	 * requested}. Requests are queued and binary chunks are requested from the {@link Publisher}. As soon as the
	 * {@link Publisher} emits items, chunks are provided to the read request which completes the number-of-written-bytes
	 * {@link Publisher}.
	 * <p>
	 * {@link AsyncInputStream} is supposed to work as sequential callback API that is called until reaching EOF.
	 * {@link #close()} is propagated as cancellation signal to the binary {@link Publisher}.
	 *
	 * @author Mark Paluch
	 */
	@RequiredArgsConstructor
	static class BinaryPublisherToAsyncInputStreamAdapter implements AsyncInputStream {

		private static final AtomicLongFieldUpdater<BinaryPublisherToAsyncInputStreamAdapter> UPDATER = AtomicLongFieldUpdater
				.newUpdater(BinaryPublisherToAsyncInputStreamAdapter.class, "demand");

		private final Publisher<DataBuffer> buffers;
		private final Context subscriberContext;
		private final DefaultDataBufferFactory factory = new DefaultDataBufferFactory();

		private final AtomicBoolean subscribed = new AtomicBoolean();
		private volatile Subscription subscription;
		private volatile boolean cancelled;
		private volatile boolean complete;
		private volatile Throwable error;
		private final Queue<BiConsumer<DataBuffer, Integer>> readRequests = Queues.<BiConsumer<DataBuffer, Integer>> small()
				.get();

		// See UPDATER
		private volatile long demand;

		/*
		 * (non-Javadoc)
		 * @see com.mongodb.reactivestreams.client.gridfs.AsyncInputStream#read(java.nio.ByteBuffer)
		 */
		@Override
		public Publisher<Integer> read(ByteBuffer dst) {

			return Mono.create(sink -> {

				readRequests.offer((db, bytecount) -> {

					try {

						if (error != null) {
							sink.error(error);
							return;
						}

						if (bytecount == -1) {
							sink.success(-1);
							return;
						}

						ByteBuffer byteBuffer = db.asByteBuffer();
						int toWrite = byteBuffer.remaining();
						dst.put(byteBuffer);

						sink.success(toWrite);
					} catch (Exception e) {
						sink.error(e);
					} finally {
						DataBufferUtils.release(db);
					}
				});

				request(1);
			});
		}

		/*
		 * (non-Javadoc)
		 * @see com.mongodb.reactivestreams.client.gridfs.AsyncInputStream#close()
		 */
		@Override
		public Publisher<Success> close() {

			return Mono.create(sink -> {
				cancelled = true;

				if (error != null) {
					sink.error(error);
					return;
				}

				sink.success(Success.SUCCESS);
			});
		}

		protected void request(int n) {

			if (complete) {
				terminatePendingReads();
				return;
			}

			Operators.addCap(UPDATER, this, n);

			if (!subscribed.get()) {

				if (subscribed.compareAndSet(false, true)) {

					buffers.subscribe(new CoreSubscriber<DataBuffer>() {

						@Override
						public Context currentContext() {
							return subscriberContext;
						}

						@Override
						public void onSubscribe(Subscription s) {
							subscription = s;

							Operators.addCap(UPDATER, BinaryPublisherToAsyncInputStreamAdapter.this, -1);
							s.request(1);
						}

						@Override
						public void onNext(DataBuffer dataBuffer) {

							if (cancelled || complete) {
								DataBufferUtils.release(dataBuffer);
								Operators.onNextDropped(dataBuffer, subscriberContext);
								return;
							}

							BiConsumer<DataBuffer, Integer> poll = readRequests.poll();

							if (poll == null) {

								DataBufferUtils.release(dataBuffer);
								Operators.onNextDropped(dataBuffer, subscriberContext);
								subscription.cancel();
								return;
							}

							poll.accept(dataBuffer, dataBuffer.readableByteCount());

							requestFromSubscription(subscription);
						}

						@Override
						public void onError(Throwable t) {

							if (cancelled || complete) {
								Operators.onErrorDropped(t, subscriberContext);
								return;
							}

							error = t;
							complete = true;
							terminatePendingReads();
						}

						@Override
						public void onComplete() {

							complete = true;
							terminatePendingReads();
						}
					});
				}
			} else {

				Subscription subscription = this.subscription;

				if (subscription != null) {
					requestFromSubscription(subscription);
				}
			}
		}

		void requestFromSubscription(Subscription subscription) {

			long demand = UPDATER.get(BinaryPublisherToAsyncInputStreamAdapter.this);

			if (cancelled) {
				subscription.cancel();
			}

			if (demand > 0 && UPDATER.compareAndSet(BinaryPublisherToAsyncInputStreamAdapter.this, demand, demand - 1)) {
				subscription.request(1);
			}
		}

		/**
		 * Terminates pending reads with empty buffers.
		 */
		void terminatePendingReads() {

			BiConsumer<DataBuffer, Integer> readers;

			while ((readers = readRequests.poll()) != null) {
				readers.accept(factory.wrap(new byte[0]), -1);
			}
		}
	}

	/**
	 * Utility to adapt a {@link AsyncInputStream} to a {@link Publisher} emitting {@link DataBuffer}.
	 */
	static class BinaryStreamUtility {

		/**
		 * Creates a {@link Publisher} emitting {@link DataBuffer}s.
		 *
		 * @param dataBufferFactory must not be {@literal null}.
		 * @param inputStream must not be {@literal null}.
		 * @return the resulting {@link Publisher}.
		 */
		public static Flux<DataBuffer> createBinaryStream(DataBufferFactory dataBufferFactory,
				AsyncInputStream inputStream) {

			AtomicBoolean closed = new AtomicBoolean();

			return Flux.push((sink) -> {

				sink.onDispose(() -> {
					close(inputStream, closed);
				});

				sink.onCancel(() -> {
					close(inputStream, closed);
				});

				emitNext(dataBufferFactory, inputStream, sink);
			});
		}

		/**
		 * Emit the next {@link DataBuffer}.
		 *
		 * @param dataBufferFactory
		 * @param inputStream
		 * @param sink
		 */
		static void emitNext(DataBufferFactory dataBufferFactory, AsyncInputStream inputStream, FluxSink<DataBuffer> sink) {

			DataBuffer dataBuffer = dataBufferFactory.allocateBuffer();
			ByteBuffer intermediate = ByteBuffer.allocate(dataBuffer.capacity());

			Mono.from(inputStream.read(intermediate)).subscribe(bytes -> {

				intermediate.flip();
				dataBuffer.write(intermediate);
				sink.next(dataBuffer);

				if (bytes == -1) {
					sink.complete();
				} else {
					emitNext(dataBufferFactory, inputStream, sink);
				}
			}, sink::error);
		}

		static void close(AsyncInputStream inputStream, AtomicBoolean closed) {
			if (closed.compareAndSet(false, true)) {
				Mono.from(inputStream.close()).subscribe();
			}
		}
	}
}
