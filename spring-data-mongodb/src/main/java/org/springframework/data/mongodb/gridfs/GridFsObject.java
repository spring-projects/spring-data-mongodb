/*
 * Copyright 2020-2023 the original author or authors.
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
import org.springframework.lang.Nullable;

import com.mongodb.client.gridfs.model.GridFSFile;

/**
 * A common interface when dealing with GridFs items using Spring Data.
 *
 * @author Christoph Strobl
 * @since 3.0
 */
public interface GridFsObject<ID, CONTENT> {

	/**
	 * The {@link GridFSFile#getId()} value converted into its simple java type. <br />
	 * A {@link org.bson.BsonString} will be converted to plain {@link String}.
	 *
	 * @return can be {@literal null} depending on the implementation.
	 */
	@Nullable
	ID getFileId();

	/**
	 * The filename.
	 *
	 * @return
	 */
	String getFilename();

	/**
	 * The actual file content.
	 *
	 * @return
	 * @throws IllegalStateException if the content cannot be obtained.
	 */
	CONTENT getContent();

	/**
	 * Additional information like file metadata (eg. contentType).
	 *
	 * @return never {@literal null}.
	 */
	Options getOptions();

	/**
	 * Additional, context relevant information.
	 *
	 * @author Christoph Strobl
	 */
	class Options {

		private final Document metadata;
		private final int chunkSize;

		private Options(Document metadata, int chunkSize) {

			this.metadata = metadata;
			this.chunkSize = chunkSize;
		}

		/**
		 * Static factory to create empty options.
		 *
		 * @return new instance of {@link Options}.
		 */
		public static Options none() {
			return new Options(new Document(), -1);
		}

		/**
		 * Static factory method to create {@link Options} with given content type.
		 *
		 * @param contentType
		 * @return new instance of {@link Options}.
		 */
		public static Options typed(String contentType) {
			return new Options(new Document("_contentType", contentType), -1);
		}

		/**
		 * Static factory method to create {@link Options} by extracting information from the given {@link GridFSFile}.
		 *
		 * @param gridFSFile can be {@literal null}, returns {@link #none()} in that case.
		 * @return new instance of {@link Options}.
		 */
		public static Options from(@Nullable GridFSFile gridFSFile) {
			return gridFSFile != null ? new Options(gridFSFile.getMetadata(), gridFSFile.getChunkSize()) : none();
		}

		/**
		 * Set the associated content type.
		 *
		 * @param contentType must not be {@literal null}.
		 * @return new instance of {@link Options}.
		 */
		public Options contentType(String contentType) {

			Options target = new Options(new Document(metadata), chunkSize);
			target.metadata.put("_contentType", contentType);
			return target;
		}

		/**
		 * @param metadata
		 * @return new instance of {@link Options}.
		 */
		public Options metadata(Document metadata) {
			return new Options(metadata, chunkSize);
		}

		/**
		 * @param chunkSize the file chunk size to use.
		 * @return new instance of {@link Options}.
		 */
		public Options chunkSize(int chunkSize) {
			return new Options(metadata, chunkSize);
		}

		/**
		 * @return never {@literal null}.
		 */
		public Document getMetadata() {
			return metadata;
		}

		/**
		 * @return the chunk size to use.
		 */
		public int getChunkSize() {
			return chunkSize;
		}

		/**
		 * @return {@literal null} if not set.
		 */
		@Nullable
		String getContentType() {
			return (String) metadata.get("_contentType");
		}
	}
}
