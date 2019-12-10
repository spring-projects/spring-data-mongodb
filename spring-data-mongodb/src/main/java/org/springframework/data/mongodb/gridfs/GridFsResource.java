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

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.mongodb.MongoGridFSException;
import com.mongodb.client.gridfs.model.GridFSFile;

/**
 * {@link GridFSFile} based {@link Resource} implementation.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Hartmut Lang
 * @author Mark Paluch
 */
public class GridFsResource extends InputStreamResource {

	static final String CONTENT_TYPE_FIELD = "_contentType";
	private static final ByteArrayInputStream EMPTY_INPUT_STREAM = new ByteArrayInputStream(new byte[0]);

	private final @Nullable GridFSFile file;
	private final String filename;

	/**
	 * Creates a new, absent {@link GridFsResource}.
	 *
	 * @param filename filename of the absent resource.
	 * @since 2.1
	 */
	private GridFsResource(String filename) {

		super(EMPTY_INPUT_STREAM, String.format("GridFs resource [%s]", filename));

		this.file = null;
		this.filename = filename;
	}

	/**
	 * Creates a new {@link GridFsResource} from the given {@link GridFSFile}.
	 *
	 * @param file must not be {@literal null}.
	 */
	public GridFsResource(GridFSFile file) {
		this(file, new ByteArrayInputStream(new byte[] {}));
	}

	/**
	 * Creates a new {@link GridFsResource} from the given {@link GridFSFile} and {@link InputStream}.
	 *
	 * @param file must not be {@literal null}.
	 * @param inputStream must not be {@literal null}.
	 */
	public GridFsResource(GridFSFile file, InputStream inputStream) {

		super(inputStream, String.format("GridFs resource [%s]", file.getFilename()));

		this.file = file;
		this.filename = file.getFilename();
	}

	/**
	 * Obtain an absent {@link GridFsResource}.
	 *
	 * @param filename filename of the absent resource, must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 2.1
	 */
	public static GridFsResource absent(String filename) {

		Assert.notNull(filename, "Filename must not be null");

		return new GridFsResource(filename);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.core.io.InputStreamResource#getInputStream()
	 */
	@Override
	public InputStream getInputStream() throws IOException, IllegalStateException {

		verifyExists();
		return super.getInputStream();
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
		return this.file;
	}

	/**
	 * Returns the {@link Resource}'s content type.
	 *
	 * @return never {@literal null}.
	 * @throws com.mongodb.MongoGridFSException in case no content type declared on {@link GridFSFile#getMetadata()} nor
	 *           provided via {@link GridFSFile}.
	 * @throws IllegalStateException if the file does not {@link #exists()}.
	 */
	@SuppressWarnings("deprecation")
	public String getContentType() {

		Assert.state(exists(), () -> String.format("%s does not exist.", getDescription()));

		return Optional.ofNullable(getGridFSFile().getMetadata()).map(it -> it.get(CONTENT_TYPE_FIELD, String.class))
				.orElseThrow(() -> new MongoGridFSException("No contentType data for this GridFS file"));
	}

	private void verifyExists() throws FileNotFoundException {

		if (!exists()) {
			throw new FileNotFoundException(String.format("%s does not exist.", getDescription()));
		}
	}
}
