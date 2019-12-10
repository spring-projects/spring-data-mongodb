/*
 * Copyright 2018-2019 the original author or authors.
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

import java.io.FileNotFoundException;
import java.util.Date;

import org.bson.BsonObjectId;
import org.bson.Document;
import org.junit.Test;

import com.mongodb.MongoGridFSException;
import com.mongodb.client.gridfs.model.GridFSFile;

/**
 * Unit tests for {@link GridFsResource}.
 *
 * @author Mark Paluch
 * @auhtor Christoph Strobl
 */
public class GridFsResourceUnitTests {

	@Test // DATAMONGO-1850
	public void shouldReadContentTypeCorrectly() {

		Document metadata = new Document(GridFsResource.CONTENT_TYPE_FIELD, "text/plain");
		GridFSFile file = new GridFSFile(new BsonObjectId(), "foo", 0, 0, new Date(), metadata);
		GridFsResource resource = new GridFsResource(file);

		assertThat(resource.getContentType()).isEqualTo("text/plain");
	}

	@Test // DATAMONGO-2240
	public void shouldReturnGridFSFile() {

		GridFSFile file = new GridFSFile(new BsonObjectId(), "foo", 0, 0, new Date(), new Document());
		GridFsResource resource = new GridFsResource(file);

		assertThat(resource.getGridFSFile()).isSameAs(file);
	}

	@Test // DATAMONGO-1850
	public void shouldThrowExceptionOnEmptyContentType() {

		GridFSFile file = new GridFSFile(new BsonObjectId(), "foo", 0, 0, new Date(), null);
		GridFsResource resource = new GridFsResource(file);

		assertThatThrownBy(resource::getContentType).isInstanceOf(MongoGridFSException.class);
	}

	@Test // DATAMONGO-1850
	public void shouldThrowExceptionOnEmptyContentTypeInMetadata() {

		GridFSFile file = new GridFSFile(new BsonObjectId(), "foo", 0, 0, new Date(), new Document());
		GridFsResource resource = new GridFsResource(file);

		assertThatThrownBy(resource::getContentType).isInstanceOf(MongoGridFSException.class);
	}

	@Test // DATAMONGO-1914
	public void gettersThrowExceptionForAbsentResource() {

		GridFsResource absent = GridFsResource.absent("foo");

		assertThat(absent.exists()).isFalse();
		assertThat(absent.getDescription()).contains("GridFs resource [foo]");

		assertThatExceptionOfType(IllegalStateException.class).isThrownBy(absent::getContentType);
		assertThatExceptionOfType(IllegalStateException.class).isThrownBy(absent::getId);

		assertThatExceptionOfType(FileNotFoundException.class).isThrownBy(absent::contentLength);
		assertThatExceptionOfType(FileNotFoundException.class).isThrownBy(absent::getInputStream);
		assertThatExceptionOfType(FileNotFoundException.class).isThrownBy(absent::lastModified);
		assertThatExceptionOfType(FileNotFoundException.class).isThrownBy(absent::getURI);
		assertThatExceptionOfType(FileNotFoundException.class).isThrownBy(absent::getURL);
	}

	@Test // DATAMONGO-1914
	public void shouldReturnFilenameForAbsentResource() {

		GridFsResource absent = GridFsResource.absent("foo");

		assertThat(absent.exists()).isFalse();
		assertThat(absent.getDescription()).contains("GridFs resource [foo]");
		assertThat(absent.getFilename()).isEqualTo("foo");
	}
}
