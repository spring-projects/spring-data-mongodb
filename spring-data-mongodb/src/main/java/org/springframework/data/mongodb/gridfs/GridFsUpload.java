/*
 * Copyright 2020-2021 the original author or authors.
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

import java.io.InputStream;
import java.util.function.Supplier;

import org.bson.Document;
import org.bson.types.ObjectId;

import org.springframework.data.util.Lazy;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StreamUtils;

import com.mongodb.client.gridfs.model.GridFSFile;

/**
 * Upload descriptor for a GridFS file upload.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 3.0
 */
public class GridFsUpload<ID> implements GridFsObject<ID, InputStream> {

	private final @Nullable ID id;
	private final Lazy<InputStream> dataStream;
	private final String filename;
	private final Options options;

	private GridFsUpload(@Nullable ID id, Lazy<InputStream> dataStream, String filename, Options options) {

		Assert.notNull(dataStream, "Data Stream must not be null");
		Assert.notNull(filename, "Filename must not be null");
		Assert.notNull(options, "Options must not be null");

		this.id = id;
		this.dataStream = dataStream;
		this.filename = filename;
		this.options = options;
	}

	/**
	 * The {@link GridFSFile#getId()} value converted into its simple java type. <br />
	 * A {@link org.bson.BsonString} will be converted to plain {@link String}.
	 *
	 * @return can be {@literal null}.
	 * @see org.springframework.data.mongodb.gridfs.GridFsObject#getFileId()
	 */
	@Override
	@Nullable
	public ID getFileId() {
		return id;
	}

	@Override
	public String getFilename() {
		return filename;
	}

	@Override
	public InputStream getContent() {
		return dataStream.orElse(StreamUtils.emptyInput());
	}

	@Override
	public Options getOptions() {
		return options;
	}

	/**
	 * Create a new instance of {@link GridFsUpload} for the given {@link InputStream}.
	 *
	 * @param stream must not be {@literal null}.
	 * @return new instance of {@link GridFsUpload}.
	 */
	public static GridFsUploadBuilder<ObjectId> fromStream(InputStream stream) {
		return new GridFsUploadBuilder<ObjectId>().content(stream);
	}

	/**
	 * Builder to create {@link GridFsUpload} in a fluent way.
	 *
	 * @param <T> the target id type.
	 */
	public static class GridFsUploadBuilder<T> {

		private Object id;
		private Lazy<InputStream> dataStream;
		private String filename;
		private Options options = Options.none();

		private GridFsUploadBuilder() {}

		/**
		 * Define the content of the file to upload.
		 *
		 * @param stream the upload content.
		 * @return this.
		 */
		public GridFsUploadBuilder<T> content(InputStream stream) {

			Assert.notNull(stream, "InputStream must not be null");

			return content(() -> stream);
		}

		/**
		 * Define the content of the file to upload.
		 *
		 * @param stream the upload content.
		 * @return this.
		 */
		public GridFsUploadBuilder<T> content(Supplier<InputStream> stream) {

			Assert.notNull(stream, "InputStream Supplier must not be null");

			this.dataStream = Lazy.of(stream);
			return this;
		}

		/**
		 * Set the id to use.
		 *
		 * @param id the id to save the content to.
		 * @param <T1>
		 * @return this.
		 */
		public <T1> GridFsUploadBuilder<T1> id(T1 id) {

			this.id = id;
			return (GridFsUploadBuilder<T1>) this;
		}

		/**
		 * Set the filename.
		 *
		 * @param filename the filename to use.
		 * @return this.
		 */
		public GridFsUploadBuilder<T> filename(String filename) {

			this.filename = filename;
			return this;
		}

		/**
		 * Set additional file information.
		 *
		 * @param options must not be {@literal null}.
		 * @return this.
		 */
		public GridFsUploadBuilder<T> options(Options options) {

			Assert.notNull(options, "Options must not be null");

			this.options = options;
			return this;
		}

		/**
		 * Set the file metadata.
		 *
		 * @param metadata must not be {@literal null}.
		 * @return this.
		 */
		public GridFsUploadBuilder<T> metadata(Document metadata) {

			this.options = this.options.metadata(metadata);
			return this;
		}

		/**
		 * Set the upload chunk size in bytes.
		 *
		 * @param chunkSize use negative number for default.
		 * @return this.
		 */
		public GridFsUploadBuilder<T> chunkSize(int chunkSize) {

			this.options = this.options.chunkSize(chunkSize);
			return this;
		}

		/**
		 * Set id, filename, metadata and chunk size from given file.
		 *
		 * @param gridFSFile must not be {@literal null}.
		 * @return this.
		 */
		public GridFsUploadBuilder<T> gridFsFile(GridFSFile gridFSFile) {

			Assert.notNull(gridFSFile, "GridFSFile must not be null");

			this.id = gridFSFile.getId();
			this.filename = gridFSFile.getFilename();
			this.options = this.options.metadata(gridFSFile.getMetadata());
			this.options = this.options.chunkSize(gridFSFile.getChunkSize());

			return this;
		}

		/**
		 * Set the content type.
		 *
		 * @param contentType must not be {@literal null}.
		 * @return this.
		 */
		public GridFsUploadBuilder<T> contentType(String contentType) {

			this.options = this.options.contentType(contentType);
			return this;
		}

		public GridFsUpload<T> build() {
			return new GridFsUpload(id, dataStream, filename, options);
		}
	}
}
