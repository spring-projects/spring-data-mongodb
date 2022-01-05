/*
 * Copyright 2019-2021 the original author or authors.
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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import org.bson.BsonValue;
import org.reactivestreams.Publisher;
import org.springframework.core.io.Resource;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.data.mongodb.util.BsonUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.reactivestreams.client.gridfs.GridFSDownloadPublisher;

/**
 * Reactive {@link GridFSFile} based {@link Resource} implementation. Note that the {@link #getDownloadStream() content}
 * can be consumed only once.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.2
 */
public class ReactiveGridFsResource implements GridFsObject<Object, Publisher<DataBuffer>> {

	private final AtomicBoolean consumed = new AtomicBoolean(false);

	private final @Nullable Object id;
	private final Options options;
	private final String filename;
	private final @Nullable GridFSDownloadPublisher downloadPublisher;
	private final DataBufferFactory dataBufferFactory;

	/**
	 * Creates a new, absent {@link ReactiveGridFsResource}.
	 *
	 * @param filename filename of the absent resource.
	 * @param downloadPublisher
	 */
	public ReactiveGridFsResource(String filename, @Nullable GridFSDownloadPublisher downloadPublisher) {
		this(null, filename, Options.none(), downloadPublisher);
	}

	/**
	 * Creates a new, absent {@link ReactiveGridFsResource}.
	 *
	 * @param id
	 * @param filename filename of the absent resource.
	 * @param options
	 * @param downloadPublisher
	 * @since 3.0
	 */
	public ReactiveGridFsResource(@Nullable Object id, String filename, Options options,
			@Nullable GridFSDownloadPublisher downloadPublisher) {
		this(id, filename, options, downloadPublisher, new DefaultDataBufferFactory());
	}

	ReactiveGridFsResource(GridFSFile file, @Nullable GridFSDownloadPublisher downloadPublisher, DataBufferFactory dataBufferFactory) {
		this(file.getId(), file.getFilename(), Options.from(file), downloadPublisher, dataBufferFactory);
	}

	/**
	 * Creates a new, absent {@link ReactiveGridFsResource}.
	 *
	 * @param id
	 * @param filename filename of the absent resource.
	 * @param options
	 * @param downloadPublisher
	 * @param dataBufferFactory
	 * @since 3.0
	 */
	ReactiveGridFsResource(@Nullable Object id, String filename, Options options,
			@Nullable GridFSDownloadPublisher downloadPublisher, DataBufferFactory dataBufferFactory) {

		this.id = id;
		this.filename = filename;
		this.options = options;
		this.downloadPublisher = downloadPublisher;
		this.dataBufferFactory = dataBufferFactory;
	}

	/**
	 * Obtain an absent {@link ReactiveGridFsResource}.
	 *
	 * @param filename filename of the absent resource, must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 2.1
	 */
	public static ReactiveGridFsResource absent(String filename) {

		Assert.notNull(filename, "Filename must not be null");
		return new ReactiveGridFsResource(filename, null);
	}

	@Override
	public Object getFileId() {
		return id instanceof BsonValue ? BsonUtils.toJavaType((BsonValue) id) : id;
	}

	/**
	 * @see org.springframework.core.io.AbstractResource#getFilename()
	 */
	public String getFilename() throws IllegalStateException {
		return this.filename;
	}

	/**
	 * @return the underlying {@link GridFSFile}. Can be {@literal null} if absent.
	 * @since 2.2
	 */
	public Mono<GridFSFile> getGridFSFile() {
		return downloadPublisher != null ? Mono.from(downloadPublisher.getGridFSFile()) : Mono.empty();
	}

	/**
	 * Obtain the data as {@link InputStream}. <br />
	 * <strong>NOTE</strong> Buffers data in memory. Use {@link #getDownloadStream()} for large files.
	 *
	 * @throws IllegalStateException if the underlying {@link Publisher} has already been consumed.
	 * @see org.springframework.core.io.InputStreamResource#getInputStream()
	 * @see #getDownloadStream()
	 * @see DataBufferUtils#join(Publisher)
	 * @since 3.0
	 */
	public Mono<InputStream> getInputStream() throws IllegalStateException {

		return getDownloadStream() //
				.transform(DataBufferUtils::join) //
				.as(Mono::from) //
				.map(DataBuffer::asInputStream);
	}

	/**
	 * Obtain the download stream emitting chunks of data as they come in. <br />
	 *
	 * @return {@link Flux#empty()} if the file does not exist.
	 * @throws IllegalStateException if the underlying {@link Publisher} has already been consumed.
	 * @see org.springframework.core.io.InputStreamResource#getInputStream()
	 * @see #getDownloadStream()
	 * @see DataBufferUtils#join(Publisher)
	 * @since 3.0
	 */
	public Flux<DataBuffer> getDownloadStream() {

		if (downloadPublisher == null) {
			return Flux.empty();
		}

		return createDownloadStream(downloadPublisher);
	}

	@Override
	public Flux<DataBuffer> getContent() {
		return getDownloadStream();
	}

	@Override
	public Options getOptions() {
		return options;
	}

	/**
	 * Obtain the download stream emitting chunks of data with given {@code chunkSize} as they come in.
	 *
	 * @param chunkSize the preferred number of bytes per emitted {@link DataBuffer}.
	 * @return {@link Flux#empty()} if the file does not exist.
	 * @throws IllegalStateException if the underlying {@link Publisher} has already been consumed.
	 * @see org.springframework.core.io.InputStreamResource#getInputStream()
	 * @see #getDownloadStream()
	 * @see DataBufferUtils#join(Publisher)
	 * @since 3.0
	 */
	public Flux<DataBuffer> getDownloadStream(int chunkSize) {

		if (downloadPublisher == null) {
			return Flux.empty();
		}

		return createDownloadStream(downloadPublisher.bufferSizeBytes(chunkSize));
	}

	private Flux<DataBuffer> createDownloadStream(GridFSDownloadPublisher publisher) {

		return Flux.from(publisher) //
				.map(dataBufferFactory::wrap) //
				.doOnSubscribe(it -> this.verifyStreamStillAvailable());
	}

	public boolean exists() {
		return downloadPublisher != null;
	}

	private void verifyStreamStillAvailable() {

		if (!consumed.compareAndSet(false, true)) {
			throw new IllegalStateException("Stream already consumed.");
		}
	}
}
