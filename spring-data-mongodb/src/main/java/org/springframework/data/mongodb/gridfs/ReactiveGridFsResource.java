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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.core.io.Resource;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.reactivestreams.client.gridfs.GridFSDownloadPublisher;

/**
 * Reactive {@link GridFSFile} based {@link Resource} implementation.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.2
 */
public class ReactiveGridFsResource {

	private final GridFSDownloadPublisher content;
	private final String filename;

	/**
	 * Creates a new, absent {@link ReactiveGridFsResource}.
	 *
	 * @param filename filename of the absent resource.
	 * @param content
	 */
	public ReactiveGridFsResource(String filename, @Nullable GridFSDownloadPublisher content) {

		this.content = content;
		this.filename = filename;
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
		return content != null ? Mono.from(content.getGridFSFile()) : Mono.empty();
	}

	/**
	 */
	public Flux<DataBuffer> getDownloadStream() {

		if (content == null) {
			return Flux.empty();
		}

		return createDownloadStream(content);
	}

	/**
	 */
	public Flux<DataBuffer> getDownloadStream(int chunkSize) {

		if (content == null) {
			return Flux.empty();

		}

		return createDownloadStream(content.bufferSizeBytes(chunkSize));
	}

	private Flux<DataBuffer> createDownloadStream(GridFSDownloadPublisher publisher) {

		DataBufferFactory bufferFactory = new DefaultDataBufferFactory();
		return Flux.from(publisher).map(bufferFactory::wrap);
	}

	public boolean exists() {
		return content != null;
	}
}
