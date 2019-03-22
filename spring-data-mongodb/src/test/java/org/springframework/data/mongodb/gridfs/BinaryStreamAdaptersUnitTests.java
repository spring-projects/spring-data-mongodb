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

import static org.assertj.core.api.Assertions.*;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.util.StreamUtils;

import com.mongodb.reactivestreams.client.gridfs.AsyncInputStream;
import com.mongodb.reactivestreams.client.gridfs.helpers.AsyncStreamHelper;

/**
 * Unit tests for {@link BinaryStreamAdapters}.
 *
 * @author Mark Paluch
 */
public class BinaryStreamAdaptersUnitTests {

	@Test // DATAMONGO-1855
	public void shouldAdaptAsyncInputStreamToDataBufferPublisher() throws IOException {

		ClassPathResource resource = new ClassPathResource("gridfs/gridfs.xml");

		byte[] content = StreamUtils.copyToByteArray(resource.getInputStream());
		AsyncInputStream inputStream = AsyncStreamHelper.toAsyncInputStream(resource.getInputStream());

		Flux<DataBuffer> dataBuffers = BinaryStreamAdapters.toPublisher(inputStream, new DefaultDataBufferFactory());

		DataBufferUtils.join(dataBuffers) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					byte[] actualContent = new byte[actual.readableByteCount()];
					actual.read(actualContent);
					assertThat(actualContent).isEqualTo(content);
				}) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1855
	public void shouldAdaptBinaryPublisherToAsyncInputStream() throws IOException {

		ClassPathResource resource = new ClassPathResource("gridfs/gridfs.xml");

		byte[] content = StreamUtils.copyToByteArray(resource.getInputStream());

		Flux<DataBuffer> dataBuffers = DataBufferUtils.readInputStream(resource::getInputStream,
				new DefaultDataBufferFactory(), 10);

		AsyncInputStream inputStream = BinaryStreamAdapters.toAsyncInputStream(dataBuffers).block();
		ByteBuffer complete = readBuffer(inputStream);

		assertThat(complete).isEqualTo(ByteBuffer.wrap(content));
	}

	static ByteBuffer readBuffer(AsyncInputStream inputStream) {

		ByteBuffer complete = ByteBuffer.allocate(1024);

		boolean hasData = true;
		while (hasData) {

			ByteBuffer chunk = ByteBuffer.allocate(100);

			Integer bytesRead = Mono.from(inputStream.read(chunk)).block();

			chunk.flip();
			complete.put(chunk);

			hasData = bytesRead > -1;
		}

		complete.flip();

		return complete;
	}
}
