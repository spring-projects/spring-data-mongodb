/*
 * Copyright 2011 the original author or authors.
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

package org.springframework.data.document.couchdb.core;

import java.util.UUID;

import junit.framework.Assert;

import org.junit.Ignore;
import org.junit.Test;
import org.springframework.data.document.couchdb.DummyDocument;

public class CouchTemplateIntegrationTests extends AbstractCouchTemplateIntegrationTests {

	
	@Test
	@Ignore("until CI has couch server running")
	public void saveAndFindTest() {
		CouchTemplate template = new CouchTemplate(CouchConstants.TEST_DATABASE_URL);
		DummyDocument document = new DummyDocument("hello");
		String id = UUID.randomUUID().toString();
		template.save(id, document);
		DummyDocument foundDocument = template.findOne(id, DummyDocument.class);
		Assert.assertEquals(document.getMessage(), foundDocument.getMessage());
	}
}
