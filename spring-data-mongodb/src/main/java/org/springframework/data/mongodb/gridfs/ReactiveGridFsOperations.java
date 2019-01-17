/*
 * Copyright 2019 the original author or authors.
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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.reactivestreams.Publisher;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.lang.Nullable;

import com.mongodb.client.gridfs.GridFSFindIterable;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.reactivestreams.client.gridfs.AsyncInputStream;

/**
 * Collection of operations to store and read files from MongoDB GridFS using reactive infrastructure.
 *
 * @author Mark Paluch
 * @since 2.2
 */
public interface ReactiveGridFsOperations {

	/**
	 * Stores the given content into a file with the given name.
	 *
	 * @param content must not be {@literal null}.
	 * @param filename must not be {@literal null} or empty.
	 * @return the {@link ObjectId} of the {@link com.mongodb.client.gridfs.model.GridFSFile} just created.
	 */
	default Mono<ObjectId> store(Publisher<DataBuffer> content, String filename) {
		return store(content, filename, (Object) null);
	}

	/**
	 * Stores the given content into a file with the given name.
	 *
	 * @param content must not be {@literal null}.
	 * @param metadata can be {@literal null}.
	 * @return the {@link ObjectId} of the {@link com.mongodb.client.gridfs.model.GridFSFile} just created.
	 */
	default Mono<ObjectId> store(Publisher<DataBuffer> content, @Nullable Object metadata) {
		return store(content, null, metadata);
	}

	/**
	 * Stores the given content into a file with the given name.
	 *
	 * @param content must not be {@literal null}.
	 * @param metadata can be {@literal null}.
	 * @return the {@link ObjectId} of the {@link com.mongodb.client.gridfs.model.GridFSFile} just created.
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
	 * @return the {@link ObjectId} of the {@link com.mongodb.client.gridfs.model.GridFSFile} just created.
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
	 * @return the {@link ObjectId} of the {@link com.mongodb.client.gridfs.model.GridFSFile} just created.
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
	 * @return the {@link ObjectId} of the {@link com.mongodb.client.gridfs.model.GridFSFile} just created.
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
	 * @return the {@link ObjectId} of the {@link com.mongodb.client.gridfs.model.GridFSFile} just created.
	 */
	Mono<ObjectId> store(Publisher<DataBuffer> content, @Nullable String filename, @Nullable String contentType,
			@Nullable Object metadata);

	/**
	 * Stores the given content into a file with the given name using the given metadata.
	 *
	 * @param content must not be {@literal null}.
	 * @param filename must not be {@literal null} or empty.
	 * @param metadata can be {@literal null}.
	 * @return the {@link ObjectId} of the {@link com.mongodb.client.gridfs.model.GridFSFile} just created.
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
	 * @return the {@link ObjectId} of the {@link com.mongodb.client.gridfs.model.GridFSFile} just created.
	 */
	Mono<ObjectId> store(AsyncInputStream content, @Nullable String filename, @Nullable String contentType,
			@Nullable Document metadata);

	Mono<ObjectId> store(Publisher<DataBuffer> content, String filename, String contentType, Document metadata);

	/**
	 * Returns all files matching the given query. Note, that currently {@link Sort} criterias defined at the
	 * {@link Query} will not be regarded as MongoDB does not support ordering for GridFS file access.
	 *
	 * @see <a href="https://jira.mongodb.org/browse/JAVA-431">MongoDB Jira: JAVA-431</a>
	 * @param query must not be {@literal null}.
	 * @return {@link GridFSFindIterable} to obtain results from. Eg. by calling
	 *         {@link GridFSFindIterable#into(java.util.Collection)}.
	 */
	Flux<GridFSFile> find(Query query);

	/**
	 * Returns a single {@link com.mongodb.client.gridfs.model.GridFSFile} matching the given query or {@literal null} in
	 * case no file matches.
	 *
	 * @param query must not be {@literal null}.
	 * @return
	 */
	Mono<GridFSFile> findOne(Query query);

	/**
	 * Deletes all files matching the given {@link Query}.
	 *
	 * @param query must not be {@literal null}.
	 */
	Mono<Void> delete(Query query);

	/**
	 * Returns the {@link GridFsResource} with the given file name.
	 *
	 * @param filename must not be {@literal null}.
	 * @return the resource. Use {@link org.springframework.core.io.Resource#exists()} to check if the returned
	 *         {@link GridFsResource} is actually present.
	 * @see ResourcePatternResolver#getResource(String)
	 */
	Mono<ReactiveGridFsResource> getResource(String filename);

	/**
	 * Returns the {@link GridFsResource} for a {@link GridFSFile}.
	 *
	 * @param file must not be {@literal null}.
	 * @return the resource for the file.
	 */
	Mono<ReactiveGridFsResource> getResource(GridFSFile file);

	/**
	 * Returns all {@link GridFsResource}s matching the given file name pattern.
	 *
	 * @param filenamePattern must not be {@literal null}.
	 * @return
	 */
	Flux<ReactiveGridFsResource> getResources(String filenamePattern);
}
