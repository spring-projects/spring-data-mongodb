/*
 * Copyright 2019-2020 the original author or authors.
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

import static org.mockito.Mockito.*;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import org.junit.Test;

import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;

/**
 * Unit tests for {@link DataBufferPublisherAdapter}.
 *
 * @author Mark Paluch
 */
public class DataBufferPublisherAdapterUnitTests {

	DataBufferFactory factory = new DefaultDataBufferFactory();

	@Test // DATAMONGO-2230
	public void adapterShouldPropagateErrors() {

		AsyncInputStreamAdapter asyncInput = mock(AsyncInputStreamAdapter.class);

		when(asyncInput.read(any())).thenReturn(Mono.just(1), Mono.error(new IllegalStateException()));
		when(asyncInput.close()).thenReturn(Mono.empty());

		Flux<DataBuffer> binaryStream = DataBufferPublisherAdapter.createBinaryStream(asyncInput, factory, 256);

		StepVerifier.create(binaryStream, 0) //
				.thenRequest(1) //
				.expectNextCount(1) //
				.thenRequest(1) //
				.verifyError(IllegalStateException.class);
	}
}
