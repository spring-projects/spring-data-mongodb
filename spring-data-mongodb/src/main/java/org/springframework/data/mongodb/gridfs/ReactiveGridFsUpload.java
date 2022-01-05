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

import org.bson.Document;
import org.bson.types.ObjectId;
import org.reactivestreams.Publisher;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.mongodb.client.gridfs.model.GridFSFile;

/**
 * Upload descriptor for a GridFS file upload.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 3.0
 */
public class ReactiveGridFsUpload<ID> implements GridFsObject<ID, Publisher<DataBuffer>> {

	private final @Nullable ID id;
	private final Publisher<DataBuffer> dataStream;
	private final String filename;
	private final Options options;

	private ReactiveGridFsUpload(@Nullable ID id, Publisher<DataBuffer> dataStream, String filename, Options options) {

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
	public Publisher<DataBuffer> getContent() {
		return dataStream;
	}

	@Override
	public Options getOptions() {
		return options;
	}

	/**
	 * Create a new instance of {@link ReactiveGridFsUpload} for the given {@link Publisher}.
	 *
	 * @param source must not be {@literal null}.
	 * @return new instance of {@link GridFsUpload}.
	 */
	public static ReactiveGridFsUploadBuilder<ObjectId> fromPublisher(Publisher<DataBuffer> source) {
		return new ReactiveGridFsUploadBuilder<ObjectId>().content(source);
	}

	/**
	 * Builder to create {@link ReactiveGridFsUpload} in a fluent way.
	 *
	 * @param <T> the target id type.
	 */
	public static class ReactiveGridFsUploadBuilder<T> {

		private @Nullable Object id;
		private Publisher<DataBuffer> dataStream;
		private String filename;
		private Options options = Options.none();

		private ReactiveGridFsUploadBuilder() {}

		/**
		 * Define the content of the file to upload.
		 *
		 * @param source the upload content.
		 * @return this.
		 */
		public ReactiveGridFsUploadBuilder<T> content(Publisher<DataBuffer> source) {
			this.dataStream = source;
			return this;
		}

		/**
		 * Set the id to use.
		 *
		 * @param id the id to save the content to.
		 * @param <T1>
		 * @return this.
		 */
		public <T1> ReactiveGridFsUploadBuilder<T1> id(T1 id) {

			this.id = id;
			return (ReactiveGridFsUploadBuilder<T1>) this;
		}

		/**
		 * Set the filename.
		 *
		 * @param filename the filename to use.
		 * @return this.
		 */
		public ReactiveGridFsUploadBuilder<T> filename(String filename) {

			this.filename = filename;
			return this;
		}

		/**
		 * Set additional file information.
		 *
		 * @param options must not be {@literal null}.
		 * @return this.
		 */
		public ReactiveGridFsUploadBuilder<T> options(Options options) {

			Assert.notNull(options, "Options must not be null");

			this.options = options;
			return this;
		}

		/**
		 * Set the file metadata.
		 *
		 * @param metadata must not be {@literal null}.
		 * @return
		 */
		public ReactiveGridFsUploadBuilder<T> metadata(Document metadata) {

			this.options = this.options.metadata(metadata);
			return this;
		}

		/**
		 * Set the upload chunk size in bytes.
		 *
		 * @param chunkSize use negative number for default.
		 * @return
		 */
		public ReactiveGridFsUploadBuilder<T> chunkSize(int chunkSize) {

			this.options = this.options.chunkSize(chunkSize);
			return this;
		}

		/**
		 * Set id, filename, metadata and chunk size from given file.
		 *
		 * @param gridFSFile must not be {@literal null}.
		 * @return this.
		 */
		public ReactiveGridFsUploadBuilder<T> gridFsFile(GridFSFile gridFSFile) {

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
		public ReactiveGridFsUploadBuilder<T> contentType(String contentType) {

			this.options = this.options.contentType(contentType);
			return this;
		}

		public ReactiveGridFsUpload<T> build() {
			return new ReactiveGridFsUpload(id, dataStream, filename, options);
		}
	}
}
