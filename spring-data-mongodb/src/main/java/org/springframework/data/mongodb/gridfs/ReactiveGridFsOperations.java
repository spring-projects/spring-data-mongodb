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

import org.bson.Document;
import org.bson.types.ObjectId;
import org.reactivestreams.Publisher;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.lang.Nullable;

import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.reactivestreams.client.gridfs.AsyncInputStream;

/**
 * Collection of operations to store and read files from MongoDB GridFS using reactive infrastructure.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.2
 */
public interface ReactiveGridFsOperations {

	/**
	 * Stores the given content into a file with the given name.
	 *
	 * @param content must not be {@literal null}.
	 * @param filename must not be {@literal null} or empty.
	 * @return a {@link Mono} emitting the {@link ObjectId} of the {@link com.mongodb.client.gridfs.model.GridFSFile} just
	 *         created.
	 */
	default Mono<ObjectId> store(Publisher<DataBuffer> content, String filename) {
		return store(content, filename, (Object) null);
	}

	/**
	 * Stores the given content into a file applying the given metadata.
	 *
	 * @param content must not be {@literal null}.
	 * @param metadata can be {@literal null}.
	 * @return a {@link Mono} emitting the {@link ObjectId} of the {@link com.mongodb.client.gridfs.model.GridFSFile} just
	 *         created.
	 */
	default Mono<ObjectId> store(Publisher<DataBuffer> content, @Nullable Object metadata) {
		return store(content, null, metadata);
	}

	/**
	 * Stores the given content into a file applying the given metadata.
	 *
	 * @param content must not be {@literal null}.
	 * @param metadata can be {@literal null}.
	 * @return a {@link Mono} emitting the {@link ObjectId} of the {@link com.mongodb.client.gridfs.model.GridFSFile} just
	 *         created.
	 */
	default Mono<ObjectId> store(Publisher<DataBuffer> content, @Nullable Document metadata) {
		return store(content, null, metadata);
	}

	/**
	 * Stores the given content into a file with the given name and content type.
	 *
	 * @param content must not be {@literal null}.
	 * @param filename must not be {@literal null} or empty.
	 * @param contentType can be {@literal null}.
	 * @return a {@link Mono} emitting the {@link ObjectId} of the {@link com.mongodb.client.gridfs.model.GridFSFile} just
	 *         created.
	 */
	default Mono<ObjectId> store(Publisher<DataBuffer> content, @Nullable String filename, @Nullable String contentType) {
		return store(content, filename, contentType, (Object) null);
	}

	/**
	 * Stores the given content into a file with the given name using the given metadata. The metadata object will be
	 * marshalled before writing.
	 *
	 * @param content must not be {@literal null}.
	 * @param filename can be {@literal null} or empty.
	 * @param metadata can be {@literal null}.
	 * @return a {@link Mono} emitting the {@link ObjectId} of the {@link com.mongodb.client.gridfs.model.GridFSFile} just
	 *         created.
	 */
	default Mono<ObjectId> store(Publisher<DataBuffer> content, @Nullable String filename, @Nullable Object metadata) {
		return store(content, filename, null, metadata);
	}

	/**
	 * Stores the given content into a file with the given name and content type using the given metadata. The metadata
	 * object will be marshalled before writing.
	 *
	 * @param content must not be {@literal null}.
	 * @param filename must not be {@literal null} or empty.
	 * @param contentType can be {@literal null}.
	 * @param metadata can be {@literal null}
	 * @return a {@link Mono} emitting the {@link ObjectId} of the {@link com.mongodb.client.gridfs.model.GridFSFile} just
	 *         created.
	 */
	Mono<ObjectId> store(AsyncInputStream content, @Nullable String filename, @Nullable String contentType,
			@Nullable Object metadata);

	/**
	 * Stores the given content into a file with the given name and content type using the given metadata. The metadata
	 * object will be marshalled before writing.
	 *
	 * @param content must not be {@literal null}.
	 * @param filename must not be {@literal null} or empty.
	 * @param contentType can be {@literal null}.
	 * @param metadata can be {@literal null}
	 * @return a {@link Mono} emitting the {@link ObjectId} of the {@link com.mongodb.client.gridfs.model.GridFSFile} just
	 *         created.
	 */
	Mono<ObjectId> store(Publisher<DataBuffer> content, @Nullable String filename, @Nullable String contentType,
			@Nullable Object metadata);

	/**
	 * Stores the given content into a file with the given name using the given metadata.
	 *
	 * @param content must not be {@literal null}.
	 * @param filename must not be {@literal null} or empty.
	 * @param metadata can be {@literal null}.
	 * @return a {@link Mono} emitting the {@link ObjectId} of the {@link com.mongodb.client.gridfs.model.GridFSFile} just
	 *         created.
	 */
	default Mono<ObjectId> store(Publisher<DataBuffer> content, @Nullable String filename, @Nullable Document metadata) {
		return store(content, filename, null, metadata);
	}

	/**
	 * Stores the given content into a file with the given name and content type using the given metadata.
	 *
	 * @param content must not be {@literal null}.
	 * @param filename must not be {@literal null} or empty.
	 * @param contentType can be {@literal null}.
	 * @param metadata can be {@literal null}.
	 * @return a {@link Mono} emitting the {@link ObjectId} of the {@link com.mongodb.client.gridfs.model.GridFSFile} just
	 *         created.
	 */
	Mono<ObjectId> store(AsyncInputStream content, @Nullable String filename, @Nullable String contentType,
			@Nullable Document metadata);

	/**
	 * Stores the given content into a file with the given name and content type using the given metadata.
	 *
	 * @param content must not be {@literal null}.
	 * @param filename must not be {@literal null} or empty.
	 * @param contentType can be {@literal null}.
	 * @param metadata can be {@literal null}.
	 * @return a {@link Mono} emitting the {@link ObjectId} of the {@link com.mongodb.client.gridfs.model.GridFSFile} just
	 *         created.
	 */
	Mono<ObjectId> store(Publisher<DataBuffer> content, @Nullable String filename, @Nullable String contentType,
			@Nullable Document metadata);

	/**
	 * Returns a {@link Flux} emitting all files matching the given query. <br />
	 * <strong>Note:</strong> Currently {@link Sort} criteria defined at the {@link Query} will not be regarded as MongoDB
	 * does not support ordering for GridFS file access.
	 *
	 * @see <a href="https://jira.mongodb.org/browse/JAVA-431">MongoDB Jira: JAVA-431</a>
	 * @param query must not be {@literal null}.
	 * @return {@link Flux#empty()} if no mach found.
	 */
	Flux<GridFSFile> find(Query query);

	/**
	 * Returns a {@link Mono} emitting a single {@link com.mongodb.client.gridfs.model.GridFSFile} matching the given
	 * query or {@link Mono#empty()} in case no file matches. <br />
	 * <strong>NOTE</strong> If more than one file matches the given query the resulting {@link Mono} emits an error. If
	 * you want to obtain the first found file use {@link #findFirst(Query)}.
	 *
	 * @param query must not be {@literal null}.
	 * @return {@link Mono#empty()} if not match found.
	 */
	Mono<GridFSFile> findOne(Query query);

	/**
	 * Returns a {@link Mono} emitting the frist {@link com.mongodb.client.gridfs.model.GridFSFile} matching the given
	 * query or {@link Mono#empty()} in case no file matches.
	 *
	 * @param query must not be {@literal null}.
	 * @return {@link Mono#empty()} if not match found.
	 */
	Mono<GridFSFile> findFirst(Query query);

	/**
	 * Deletes all files matching the given {@link Query}.
	 *
	 * @param query must not be {@literal null}.
	 * @return a {@link Mono} signalling operation completion.
	 */
	Mono<Void> delete(Query query);

	/**
	 * Returns a {@link Mono} emitting the {@link ReactiveGridFsResource} with the given file name.
	 *
	 * @param filename must not be {@literal null}.
	 * @return {@link Mono#empty()} if no match found.
	 */
	Mono<ReactiveGridFsResource> getResource(String filename);

	/**
	 * Returns a {@link Mono} emitting the {@link ReactiveGridFsResource} for a {@link GridFSFile}.
	 *
	 * @param file must not be {@literal null}.
	 * @return {@link Mono#empty()} if no match found.
	 */
	Mono<ReactiveGridFsResource> getResource(GridFSFile file);

	/**
	 * Returns a {@link Flux} emitting all {@link ReactiveGridFsResource}s matching the given file name pattern.
	 *
	 * @param filenamePattern must not be {@literal null}.
	 * @return {@link Flux#empty()} if no match found.
	 */
	Flux<ReactiveGridFsResource> getResources(String filenamePattern);
}
