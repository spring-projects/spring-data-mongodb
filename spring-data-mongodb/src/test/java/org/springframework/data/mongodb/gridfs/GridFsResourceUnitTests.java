/*
 * Copyright 2018 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

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
 */
public class GridFsResourceUnitTests {

	@Test // DATAMONGO-1850
	public void shouldReadContentTypeCorrectly() {

		Document metadata = new Document(GridFsResource.CONTENT_TYPE_FIELD, "text/plain");
		GridFSFile file = new GridFSFile(new BsonObjectId(), "foo", 0, 0, new Date(), "foo", metadata);
		GridFsResource resource = new GridFsResource(file);

		assertThat(resource.getContentType()).isEqualTo("text/plain");
	}

	@Test // DATAMONGO-1850
	public void shouldThrowExceptionOnEmptyContentType() {

		GridFSFile file = new GridFSFile(new BsonObjectId(), "foo", 0, 0, new Date(), "foo", null);
		GridFsResource resource = new GridFsResource(file);

		assertThatThrownBy(resource::getContentType).isInstanceOf(MongoGridFSException.class);
	}
}
