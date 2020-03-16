/*
 * Copyright 2020 the original author or authors.
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

import java.io.IOException;
import java.io.InputStream;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.data.util.Lazy;
import org.springframework.lang.Nullable;

import com.mongodb.client.gridfs.model.GridFSFile;

/**
 * @author Christoph Strobl
 * @since 3.0
 */
public class GridFsUpload<ID> implements GridFsObject<ID, InputStream> {

	private static final InputStream EMPTY_STREAM = new InputStream() {
		@Override
		public int read() throws IOException {
			return -1;
		}
	};

	private ID id;
	private Lazy<InputStream> dataStream;
	private String filename;
	private Options options;

	/**
	 * The {@link GridFSFile#getId()} value converted into its simple java type. <br />
	 * A {@link org.bson.BsonString} will be converted to plain {@link String}.
	 * 
	 * @return can be {@literal null}.
	 * @see org.springframework.data.mongodb.gridfs.GridFsObject#getFileId()
	 */
	@Override
	public ID getFileId() {
		return id;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.gridfs.GridFsObject#getFielname()
	 */
	@Override
	public String getFilename() {
		return filename;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.gridfs.GridFsObject#getContent()
	 */
	@Override
	public InputStream getContent() {
		return dataStream.orElse(EMPTY_STREAM);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.gridfs.GridFsObject#getOptions()
	 */
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
		return new GridFsUploadBuilder().content(stream);
	}

	/**
	 * Builder to create {@link GridFsUpload} in a fluent way.
	 * 
	 * @param <T> the target id type.
	 */
	public static class GridFsUploadBuilder<T> {

		private GridFsUpload upload;

		public GridFsUploadBuilder() {
			this.upload = new GridFsUpload();
			this.upload.options = Options.none();
		}

		/**
		 * Define the content of the file to upload.
		 *
		 * @param stream the upload content.
		 * @return this.
		 */
		public GridFsUploadBuilder<T> content(InputStream stream) {

			upload.dataStream = Lazy.of(() -> stream);
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

			upload.id = id;
			return (GridFsUploadBuilder<T1>) this;
		}

		/**
		 * Set the filename.
		 *
		 * @param filename the filename to use.
		 * @return this.
		 */
		public GridFsUploadBuilder<T> filename(String filename) {

			upload.filename = filename;
			return this;
		}

		/**
		 * Set additional file information.
		 *
		 * @param options must not be {@literal null}.
		 * @return this.
		 */
		public GridFsUploadBuilder<T> options(Options options) {

			upload.options = options;
			return this;
		}

		/**
		 * Set the file metadata.
		 *
		 * @param metadata must not be {@literal null}.
		 * @return
		 */
		public GridFsUploadBuilder<T> metadata(Document metadata) {

			upload.options = upload.options.metadata(metadata);
			return this;
		}

		/**
		 * Set the upload chunk size in bytes.
		 *
		 * @param chunkSize use negative number for default.
		 * @return this.
		 */
		public GridFsUploadBuilder<T> chunkSize(int chunkSize) {

			upload.options = upload.options.chunkSize(chunkSize);
			return this;
		}

		/**
		 * Set id, filename, metadata and chunk size from given file.
		 * 
		 * @param gridFSFile must not be {@literal null}.
		 * @return this.
		 */
		public GridFsUploadBuilder<T> gridFsFile(GridFSFile gridFSFile) {

			upload.id = gridFSFile.getId();
			upload.filename = gridFSFile.getFilename();
			upload.options = upload.options.metadata(gridFSFile.getMetadata());
			upload.options = upload.options.chunkSize(gridFSFile.getChunkSize());

			return this;
		}

		/**
		 * Set the content type.
		 * 
		 * @param contentType must not be {@literal null}.
		 * @return this.
		 */
		public GridFsUploadBuilder<T> contentType(String contentType) {

			upload.options = upload.options.contentType(contentType);
			return this;
		}

		public GridFsUpload<T> build() {
			return (GridFsUpload<T>) upload;
		}
	}
}
