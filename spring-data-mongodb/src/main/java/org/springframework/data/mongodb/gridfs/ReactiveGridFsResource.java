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

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

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
 * @since 2.2
 */
public class ReactiveGridFsResource extends AbstractResource {

	static final String CONTENT_TYPE_FIELD = "_contentType";
	private static final ByteArrayInputStream EMPTY_INPUT_STREAM = new ByteArrayInputStream(new byte[0]);

	private final @Nullable GridFSFile file;
	private final String filename;
	private final Flux<DataBuffer> content;

	/**
	 * Creates a new, absent {@link ReactiveGridFsResource}.
	 *
	 * @param filename filename of the absent resource.
	 * @param content
	 * @since 2.1
	 */
	private ReactiveGridFsResource(String filename, Publisher<DataBuffer> content) {

		this.file = null;
		this.filename = filename;
		this.content = Flux.from(content);
	}

	/**
	 * Creates a new {@link ReactiveGridFsResource} from the given {@link GridFSFile}.
	 *
	 * @param file must not be {@literal null}.
	 * @param content
	 */
	public ReactiveGridFsResource(GridFSFile file, Publisher<DataBuffer> content) {

		this.file = file;
		this.filename = file.getFilename();
		this.content = Flux.from(content);
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
		return file.getLength();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.core.io.AbstractResource#getFilename()
	 */
	@Override
	public String getFilename() throws IllegalStateException {
		return filename;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.core.io.AbstractResource#exists()
	 */
	@Override
	public boolean exists() {
		return file != null;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.core.io.AbstractResource#lastModified()
	 */
	@Override
	public long lastModified() throws IOException {

		verifyExists();
		return file.getUploadDate().getTime();
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

		return file.getId();
	}

	/**
	 * Retrieve the download stream.
	 *
	 * @return
	 */
	public Flux<DataBuffer> getDownloadStream() {

		if (!exists()) {
			return Flux.error(new FileNotFoundException(String.format("%s does not exist.", getDescription())));
		}
		return content;
	}

	private void verifyExists() throws FileNotFoundException {

		if (!exists()) {
			throw new FileNotFoundException(String.format("%s does not exist.", getDescription()));
		}
	}
}
