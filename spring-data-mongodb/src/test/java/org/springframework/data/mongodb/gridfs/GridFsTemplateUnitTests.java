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

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.convert.MongoConverter;

/**
 * @author Christoph Strobl
 */
class GridFsTemplateUnitTests {

	private GridFsTemplateStub template;

	@BeforeEach
	void beforeEach() {
		template = new GridFsTemplateStub();
	}

	@Test // DATAMONGO-2574
	void contentMetadataDoesNotOverrideContentTypeIfSet() {

		template.onStoreReturn(new ObjectId());
		template.store(new ByteArrayInputStream(new byte[] {}), "filename", "json", new Document("meta", "data"));

		assertThat(template.capturedUpload().getOptions().getContentType()).isEqualTo("json");
		assertThat(template.capturedUpload().getOptions().getMetadata()).containsEntry("meta", "data");
	}

	private static class GridFsTemplateStub extends GridFsTemplate {

		private Object onStoreResult;
		private GridFsObject<?, InputStream> capturedUpload;

		GridFsTemplateStub() {
			super(mock(MongoDatabaseFactory.class), mock(MongoConverter.class));
		}

		@Override
		public <T> T store(GridFsObject<T, InputStream> upload) {

			this.capturedUpload = upload;
			return (T) onStoreResult;
		}

		GridFsTemplateStub onStoreReturn(Object result) {

			this.onStoreResult = result;
			return this;
		}

		GridFsObject<?, InputStream> capturedUpload() {
			return capturedUpload;
		}
	}
}
