/*
 * Copyright 2019-2020 the original author or authors.
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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.IntFunction;

import org.reactivestreams.Publisher;
import org.springframework.core.io.AbstractResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.mongodb.client.gridfs.model.GridFSFile;

/**
 * Reactive {@link GridFSFile} based {@link Resource} implementation.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.2
 */
public class ReactiveGridFsResource extends AbstractResource {

	private static final Integer DEFAULT_CHUNK_SIZE = 256 * 1024;

	private final @Nullable GridFSFile file;
	private final String filename;
	private final IntFunction<Flux<DataBuffer>> contentFunction;

	/**
	 * Creates a new, absent {@link ReactiveGridFsResource}.
	 *
	 * @param filename filename of the absent resource.
	 * @param content
	 */
	private ReactiveGridFsResource(String filename, Publisher<DataBuffer> content) {

		this.file = null;
		this.filename = filename;
		this.contentFunction = any -> Flux.from(content);
	}

	/**
	 * Creates a new {@link ReactiveGridFsResource} from the given {@link GridFSFile}.
	 *
	 * @param file must not be {@literal null}.
	 * @param content
	 */
	public ReactiveGridFsResource(GridFSFile file, Publisher<DataBuffer> content) {
		this(file, (IntFunction<Flux<DataBuffer>>) any -> Flux.from(content));
	}

	/**
	 * Creates a new {@link ReactiveGridFsResource} from the given {@link GridFSFile}.
	 *
	 * @param file must not be {@literal null}.
	 * @param contentFunction
	 * @since 2.2.1
	 */
	ReactiveGridFsResource(GridFSFile file, IntFunction<Flux<DataBuffer>> contentFunction) {

		this.file = file;
		this.filename = file.getFilename();
		this.contentFunction = contentFunction;
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

		return new ReactiveGridFsResource(filename, Flux.empty());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.core.io.InputStreamResource#getInputStream()
	 */
	@Override
	public InputStream getInputStream() throws IllegalStateException {
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.core.io.AbstractResource#contentLength()
	 */
	@Override
	public long contentLength() throws IOException {

		verifyExists();
		return getGridFSFile().getLength();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.core.io.AbstractResource#getFilename()
	 */
	@Override
	public String getFilename() throws IllegalStateException {
		return this.filename;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.core.io.AbstractResource#exists()
	 */
	@Override
	public boolean exists() {
		return this.file != null;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.core.io.AbstractResource#lastModified()
	 */
	@Override
	public long lastModified() throws IOException {

		verifyExists();
		return getGridFSFile().getUploadDate().getTime();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.core.io.AbstractResource#getDescription()
	 */
	@Override
	public String getDescription() {
		return String.format("GridFs resource [%s]", this.getFilename());
	}

	/**
	 * Returns the {@link Resource}'s id.
	 *
	 * @return never {@literal null}.
	 * @throws IllegalStateException if the file does not {@link #exists()}.
	 */
	public Object getId() {

		Assert.state(exists(), () -> String.format("%s does not exist.", getDescription()));

		return getGridFSFile().getId();
	}

	/**
	 * @return the underlying {@link GridFSFile}. Can be {@literal null} if absent.
	 * @since 2.2
	 */
	@Nullable
	public GridFSFile getGridFSFile() {
		return file;
	}

	/**
	 * Retrieve the download stream using the default chunk size of 256 kB.
	 *
	 * @return a {@link Flux} emitting data chunks one by one. Please make sure to
	 *         {@link org.springframework.core.io.buffer.DataBufferUtils#release(DataBuffer) release} all
	 *         {@link DataBuffer buffers} when done.
	 */
	public Flux<DataBuffer> getDownloadStream() {
		return getDownloadStream(DEFAULT_CHUNK_SIZE);
	}

	/**
	 * Retrieve the download stream.
	 *
	 * @param chunkSize chunk size in bytes to use.
	 * @return a {@link Flux} emitting data chunks one by one. Please make sure to
	 *         {@link org.springframework.core.io.buffer.DataBufferUtils#release(DataBuffer) release} all
	 *         {@link DataBuffer buffers} when done.
	 * @since 2.2.1
	 */
	public Flux<DataBuffer> getDownloadStream(int chunkSize) {

		if (!exists()) {
			return Flux.error(new FileNotFoundException(String.format("%s does not exist.", getDescription())));
		}

		return contentFunction.apply(chunkSize);
	}

	private void verifyExists() throws FileNotFoundException {

		if (!exists()) {
			throw new FileNotFoundException(String.format("%s does not exist.", getDescription()));
		}
	}
}
