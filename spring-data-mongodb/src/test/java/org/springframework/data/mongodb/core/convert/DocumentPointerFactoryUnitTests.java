/*
 * Copyright 2021-2024 the original author or authors.
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
package org.springframework.data.mongodb.core.convert;

import static org.assertj.core.api.Assertions.*;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.mongodb.core.convert.DocumentPointerFactory.LinkageDocument;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;

/**
 * @author Christoph Strobl
 */
public class DocumentPointerFactoryUnitTests {

	@Test // GH-3602
	void errorsOnMongoOperatorUsage() {

		LinkageDocument source = LinkageDocument.from("{ '_id' : { '$eq' : 1 } }");

		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
				.isThrownBy(() -> getPointerValue(source, new Book())) //
				.withMessageContaining("$eq");
	}

	@Test // GH-3602
	void computesStaticPointer() {

		LinkageDocument source = LinkageDocument.from("{ '_id' : 1 }");

		assertThat(getPointerValue(source, new Book())).isEqualTo(new Document("_id", 1));
	}

	@Test // GH-3602
	void computesPointerWithIdValuePlaceholder() {

		LinkageDocument source = LinkageDocument.from("{ '_id' : ?#{id} }");

		assertThat(getPointerValue(source, new Book("book-1", null, null))).isEqualTo(new Document("id", "book-1"));
	}

	@Test // GH-3602
	void computesPointerForNonIdValuePlaceholder() {

		LinkageDocument source = LinkageDocument.from("{ 'title' : ?#{book_title} }");

		assertThat(getPointerValue(source, new Book("book-1", "Living With A Seal", null)))
				.isEqualTo(new Document("book_title", "Living With A Seal"));
	}

	@Test // GH-3602
	void computesPlaceholderFromNestedPathValue() {

		LinkageDocument source = LinkageDocument.from("{ 'metadata.pages' : ?#{p} } }");

		assertThat(getPointerValue(source, new Book("book-1", "Living With A Seal", null, new Metadata(272))))
				.isEqualTo(new Document("p", 272));
	}

	@Test // GH-3602
	void computesNestedPlaceholderPathValue() {

		LinkageDocument source = LinkageDocument.from("{ 'metadata' : { 'pages' : ?#{metadata.pages} } }");

		assertThat(getPointerValue(source, new Book("book-1", "Living With A Seal", null, new Metadata(272))))
				.isEqualTo(new Document("metadata", new Document("pages", 272)));
	}

	Object getPointerValue(LinkageDocument linkageDocument, Object value) {

		MongoMappingContext mappingContext = new MongoMappingContext();
		MongoPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(value.getClass());
		return linkageDocument
				.getDocumentPointer(mappingContext, persistentEntity, persistentEntity.getPropertyPathAccessor(value))
				.getPointer();
	}

	@Data
	@AllArgsConstructor
	@NoArgsConstructor
	static class Book {
		String id;
		String title;
		List<Author> author;
		Metadata metadata;

		public Book(String id, String title, List<Author> author) {
			this.id = id;
			this.title = title;
			this.author = author;
		}
	}

	static class Metadata {

		int pages;

		public Metadata(int pages) {
			this.pages = pages;
		}

		public int getPages() {
			return pages;
		}

		public void setPages(int pages) {
			this.pages = pages;
		}
	}

	@Data
	static class Author {
		String id;
		String firstname;
		String lastname;
	}
}
