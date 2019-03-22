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

import org.reactivestreams.Publisher;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DataBufferUtils;

import com.mongodb.reactivestreams.client.gridfs.AsyncInputStream;

/**
 * Utility methods to create adapters from between {@link Publisher} of {@link DataBuffer} and {@link AsyncInputStream}.
 *
 * @author Mark Paluch
 * @since 2.2
 */
class BinaryStreamAdapters {

	/**
	 * Creates a {@link Flux} emitting {@link DataBuffer} by reading binary chunks from {@link AsyncInputStream}.
	 * Publisher termination (completion, error, cancellation) closes the {@link AsyncInputStream}.
	 * <p/>
	 * The resulting {@link org.reactivestreams.Publisher} filters empty binary chunks and uses {@link DataBufferFactory}
	 * settings to determine the chunk size.
	 *
	 * @param inputStream must not be {@literal null}.
	 * @param dataBufferFactory must not be {@literal null}.
	 * @return {@link Flux} emitting {@link DataBuffer}s.
	 * @see DataBufferFactory#allocateBuffer()
	 */
	static Flux<DataBuffer> toPublisher(AsyncInputStream inputStream, DataBufferFactory dataBufferFactory) {

		return DataBufferPublisherAdapter.createBinaryStream(inputStream, dataBufferFactory) //
				.filter(it -> {

					if (it.readableByteCount() == 0) {
						DataBufferUtils.release(it);
						return false;
					}
					return true;
				});
	}

	/**
	 * Creates a {@link Mono} emitting a {@link AsyncInputStream} to consume a {@link Publisher} emitting
	 * {@link DataBuffer} and exposing the binary stream through {@link AsyncInputStream}. {@link DataBuffer}s are
	 * released by the adapter during consumption.
	 * <p/>
	 * This method returns a {@link Mono} to retain the {@link reactor.util.context.Context subscriber context}.
	 *
	 * @param dataBuffers must not be {@literal null}.
	 * @return {@link Mono} emitting {@link AsyncInputStream}.
	 * @see DataBufferUtils#release(DataBuffer)
	 */
	static Mono<AsyncInputStream> toAsyncInputStream(Publisher<? extends DataBuffer> dataBuffers) {

		return Mono.create(sink -> {
			sink.success(new AsyncInputStreamAdapter(dataBuffers, sink.currentContext()));
		});
	}
}
