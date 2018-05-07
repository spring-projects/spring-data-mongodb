/*
 * Copyright 2011-2018 the original author or authors.
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.data.util.Optionals;
import org.springframework.lang.Nullable;

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

	private static final GridFsResource ABSENT = new GridFsResource() {

		@Override
		public boolean exists() {
			return false;
		}

	};

	static final String CONTENT_TYPE_FIELD = "_contentType";

	private final @Nullable GridFSFile file;

	private GridFsResource() {

		super(new ByteArrayInputStream(new byte[] {}));
		file = null;
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

		super(inputStream);
		this.file = file;
	}

	/**
	 * Obtain an absent {@link GridFsResource}.
	 *
	 * @return never {@literal null}.
	 * @since 2.1
	 */
	public static GridFsResource absent() {
		return ABSENT;
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

		verifyExists();
		return file.getFilename();
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

	/**
	 * Returns the {@link Resource}'s id.
	 *
	 * @return never {@literal null}.
	 */
	public Object getId() {

		verifyExists();
		return file.getId();
	}

	/**
	 * Returns the {@link Resource}'s content type.
	 *
	 * @return never {@literal null}.
	 * @throws com.mongodb.MongoGridFSException in case no content type declared on {@link GridFSFile#getMetadata()} nor
	 *           provided via {@link GridFSFile#getContentType()}.
	 */
	@SuppressWarnings("deprecation")
	public String getContentType() {

		verifyExists();

		return Optionals
				.firstNonEmpty(
						() -> Optional.ofNullable(file.getMetadata()).map(it -> it.get(CONTENT_TYPE_FIELD, String.class)),
						() -> Optional.ofNullable(file.getContentType()))
				.orElseThrow(() -> new MongoGridFSException("No contentType data for this GridFS file"));
	}

	private void verifyExists() {

		if (!exists()) {
			throw new IllegalStateException("The resource does not exist.");
		}
	}
}
