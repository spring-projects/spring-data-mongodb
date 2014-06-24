/*
 * Copyright 2014 the original author or authors.
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
package org.springframework.data.mongodb.core.index;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.List;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.config.AbstractIntegrationTests;
import org.springframework.data.mongodb.core.IndexOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Language;
import org.springframework.data.mongodb.test.util.MongoVersionRule;
import org.springframework.data.util.Version;

import com.mongodb.WriteConcern;

/**
 * @author Christoph Strobl
 */
public class TextIndexTests extends AbstractIntegrationTests {

	public static @ClassRule MongoVersionRule version = MongoVersionRule.atLeast(new Version(2, 6));

	private @Autowired MongoTemplate template;
	private IndexOperations indexOps;

	@Before
	public void setUp() throws Exception {

		template.setWriteConcern(WriteConcern.FSYNC_SAFE);
		this.indexOps = template.indexOps(TextIndexedDocumentRoot.class);
	}

	/**
	 * @see DATAMONGO-937
	 */
	@Test
	public void indexInfoShouldHaveBeenCreatedCorrectly() {

		List<IndexInfo> indexInfos = indexOps.getIndexInfo();

		assertThat(indexInfos.size(), is(2));

		List<IndexField> fields = indexInfos.get(0).getIndexFields();
		assertThat(fields.size(), is(1));
		assertThat(fields, hasItem(IndexField.create("_id", Direction.ASC)));

		IndexInfo textIndexInfo = indexInfos.get(1);
		List<IndexField> textIndexFields = textIndexInfo.getIndexFields();
		assertThat(textIndexFields.size(), is(4));
		assertThat(textIndexFields, hasItem(IndexField.text("textIndexedPropertyWithDefaultWeight", 1F)));
		assertThat(textIndexFields, hasItem(IndexField.text("textIndexedPropertyWithWeight", 5F)));
		assertThat(textIndexFields, hasItem(IndexField.text("nestedDocument.textIndexedPropertyInNestedDocument", 1F)));
		assertThat(textIndexFields, hasItem(IndexField.create("_ftsx", Direction.ASC)));
		assertThat(textIndexInfo.getLanguage(), is("spanish"));
	}

	@Document(language = "spanish")
	static class TextIndexedDocumentRoot {

		@TextIndexed String textIndexedPropertyWithDefaultWeight;
		@TextIndexed(weight = 5) String textIndexedPropertyWithWeight;

		TextIndexedDocumentWihtLanguageOverride nestedDocument;
	}

	static class TextIndexedDocumentWihtLanguageOverride {

		@Language String lang;

		@TextIndexed String textIndexedPropertyInNestedDocument;

		String nonTextIndexedProperty;
	}
}
