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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.convert.MongoConverter;

/**
 * @author Christoph Strobl
 */
class ReactiveGridFsTemplateUnitTests {

	private ReactiveGridFsTemplateStub template;

	@BeforeEach
	void beforeEach() {
		template = new ReactiveGridFsTemplateStub();
	}

	@Test // DATAMONGO-2574
	void contentMetadataDoesNotOverrideContentTypeIfSet() {

		template.onStoreReturn(new ObjectId());
		template.store(Flux.empty(), "filename", "json", new Document("meta", "data"));

		assertThat(template.capturedUpload().getOptions().getContentType()).isEqualTo("json");
		assertThat(template.capturedUpload().getOptions().getMetadata()).containsEntry("meta", "data");
	}

	private static class ReactiveGridFsTemplateStub extends ReactiveGridFsTemplate {

		private Object onStoreResult;
		private GridFsObject<?, Publisher<DataBuffer>> capturedUpload;

		ReactiveGridFsTemplateStub() {
			super(mock(ReactiveMongoDatabaseFactory.class), mock(MongoConverter.class));
		}

		@Override
		public <T> Mono<T> store(GridFsObject<T, Publisher<DataBuffer>> upload) {

			capturedUpload = upload;
			return Mono.just((T) onStoreResult);
		}

		ReactiveGridFsTemplateStub onStoreReturn(Object result) {

			this.onStoreResult = result;
			return this;
		}

		GridFsObject<?, Publisher<DataBuffer>> capturedUpload() {
			return capturedUpload;
		}
	}
}
