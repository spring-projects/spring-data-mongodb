/*
 * Copyright 2014-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.index;

import static org.assertj.core.api.Assertions.*;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.CollectionOptions;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Language;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.test.util.MongoTemplateExtension;
import org.springframework.data.mongodb.test.util.MongoTestTemplate;
import org.springframework.data.mongodb.test.util.Template;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@ExtendWith(MongoTemplateExtension.class)
public class TextIndexTests {

	@Template(initialEntitySet = TextIndexedDocumentRoot.class)
	static MongoTestTemplate template;

	private IndexOperations indexOps;

	@BeforeEach
	public void beforeEach() throws Exception {

		this.indexOps = template.indexOps(TextIndexedDocumentRoot.class);

		template.dropDatabase();

		template.createCollection(TextIndexedDocumentRoot.class,
				CollectionOptions.empty().collation(Collation.of("de_AT")));
	}

	@Test // DATAMONGO-937, DATAMONGO-2316
	public void indexInfoShouldHaveBeenCreatedCorrectly() {

		IndexResolver indexResolver = IndexResolver.create(template.getConverter().getMappingContext());

		for (IndexDefinition indexDefinition : indexResolver.resolveIndexFor(TextIndexedDocumentRoot.class)) {
			indexOps.ensureIndex(indexDefinition);
		}

		List<IndexInfo> indexInfos = indexOps.getIndexInfo();

		assertThat(indexInfos.size()).isEqualTo(2);

		List<IndexField> fields = indexInfos.get(0).getIndexFields();
		assertThat(fields).containsExactly(IndexField.create("_id", Direction.ASC));

		IndexInfo textIndexInfo = indexInfos.get(1);
		List<IndexField> textIndexFields = textIndexInfo.getIndexFields();
		assertThat(textIndexFields).hasSize(4).contains(IndexField.text("textIndexedPropertyWithDefaultWeight", 1F),
				IndexField.text("textIndexedPropertyWithWeight", 5F),
				IndexField.text("nestedDocument.textIndexedPropertyInNestedDocument", 1F),
				IndexField.create("_ftsx", Direction.ASC));
		assertThat(textIndexInfo.getLanguage()).isEqualTo("spanish");
	}

	@Document(language = "spanish", collation = "de_AT")
	static class TextIndexedDocumentRoot {

		@TextIndexed String textIndexedPropertyWithDefaultWeight;
		@TextIndexed(weight = 5) String textIndexedPropertyWithWeight;

		TextIndexedDocumentWithLanguageOverride nestedDocument;
	}

	static class TextIndexedDocumentWithLanguageOverride {

		@Language String lang;

		@TextIndexed String textIndexedPropertyInNestedDocument;

		String nonTextIndexedProperty;
	}
}
