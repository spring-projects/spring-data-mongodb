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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.util.Date;

import org.bson.BsonObjectId;
import org.bson.BsonValue;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;

import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.reactivestreams.client.gridfs.GridFSBucket;
import com.mongodb.reactivestreams.client.gridfs.GridFSDownloadPublisher;

/**
 * Unit tests for {@link ReactiveGridFsResource}.
 *
 * @author Christoph Strobl
 * @author Mathieu Ouellet
 */
@ExtendWith(MockitoExtension.class)
class ReactiveGridFsResourceUnitTests {

	@Mock private GridFSBucket bucket;

	@Test // DATAMONGO-2427
	void streamCanOnlyBeConsumedOnce() {

		when(bucket.downloadToPublisher(any(BsonValue.class))).thenReturn(new StubGridFSDownloadPublisher());
		GridFSFile file = new GridFSFile(new BsonObjectId(), "file.name", 0, 0, new Date(), new Document());
		ReactiveGridFsResource resource = new ReactiveGridFsResource(file, Mono.just(bucket),
				new DefaultDataBufferFactory());

		assertThat(resource.exists()).isTrue();

		resource.getInputStream().as(StepVerifier::create).verifyComplete();
		resource.getInputStream().as(StepVerifier::create).verifyError(IllegalStateException.class);
		resource.getDownloadStream().as(StepVerifier::create).verifyError(IllegalStateException.class);
	}

	@Test // DATAMONGO-2427
	void existReturnsFalseForNullGridFSFile() {

		ReactiveGridFsResource resource = new ReactiveGridFsResource("file.name", Mono.just(bucket));

		assertThat(resource.exists()).isFalse();
	}

	@Test // DATAMONGO-2427
	void nonExistingResourceProducesEmptyDownloadStream() {

		ReactiveGridFsResource resource = new ReactiveGridFsResource("file.name", Mono.just(bucket));

		resource.getInputStream().as(StepVerifier::create).verifyComplete();
		resource.getInputStream().as(StepVerifier::create).verifyComplete();
		resource.getDownloadStream().as(StepVerifier::create).verifyComplete();
	}

	private static class StubGridFSDownloadPublisher implements GridFSDownloadPublisher {

		@Override
		public Publisher<GridFSFile> getGridFSFile() {
			return Mono.empty();
		}

		@Override
		public GridFSDownloadPublisher bufferSizeBytes(int bufferSizeBytes) {
			return null;
		}

		@Override
		public void subscribe(Subscriber<? super ByteBuffer> s) {

			s.onSubscribe(new Subscription() {
				@Override
				public void request(long n) {
					s.onComplete();
				}

				@Override
				public void cancel() {

				}
			});
		}
	}
}
