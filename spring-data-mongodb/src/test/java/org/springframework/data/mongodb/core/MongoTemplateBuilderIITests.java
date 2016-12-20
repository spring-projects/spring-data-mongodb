/*
 * Copyright 2016. the original author or authors.
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
package org.springframework.data.mongodb.core;

import static org.springframework.data.mongodb.core.MongoTemplateBuilderII.*;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

import java.util.List;

import com.mongodb.MongoClient;
import lombok.Data;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.query.Update;

/**
 * @author Christoph Strobl
 * @since 2016/12
 */
public class MongoTemplateBuilderIITests {

	private static final String COLLECTION_NAME = "collection-1";
	MongoTemplate template;

	@Before
	public void setUp() {

		template = new MongoTemplate(new SimpleMongoDbFactory(new MongoClient(), "test"));

		RootDocumentClass c1 = new RootDocumentClass();
		c1.firstname = "han";
		c1.id = "id-1";

		template.save(c1);
	}

	@Test
	public void allMethods() {

		//-- FINDERS
		MongoTemplateBuilderII.query(RootDocumentClass.class).all();
		MongoTemplateBuilderII.query(RootDocumentClass.class).in(COLLECTION_NAME).all();
		MongoTemplateBuilderII.query(RootDocumentClass.class).in(COLLECTION_NAME).returningResultAs(ReturnTypeClass.class).all();
		MongoTemplateBuilderII.query(RootDocumentClass.class).returningResultAs(ReturnTypeClass.class).all();

		MongoTemplateBuilderII.query(RootDocumentClass.class).findBy(query(where("firstname").is("han")));
		MongoTemplateBuilderII.query(RootDocumentClass.class).in(COLLECTION_NAME).findBy(query(where("firstname").is("han")));
		MongoTemplateBuilderII.query(RootDocumentClass.class).in(COLLECTION_NAME).returningResultAs(ReturnTypeClass.class).findBy(query(where("firstname").is("han")));
		MongoTemplateBuilderII.query(RootDocumentClass.class).returningResultAs(ReturnTypeClass.class).findBy(query(where("firstname").is("han")));

		//-- UPDATES
		update(RootDocumentClass.class).apply(new Update().set("firstname", "hunts alone"));
		update(RootDocumentClass.class).in(COLLECTION_NAME).apply(new Update().set("firstname", "hunts alone"));
		update(RootDocumentClass.class).where(query(where("firstname").is("han"))).apply(new Update().set("firstname", "hunts alone"));
		update(RootDocumentClass.class).where(query(where("firstname").is("han"))).single().apply(new Update().set("firstname", "hunts alone"));
		update(RootDocumentClass.class).where(query(where("firstname").is("han"))).upsert().apply(new Update().set("firstname", "hunts alone"));
	}

	@Data
	@org.springframework.data.mongodb.core.mapping.Document(collection = COLLECTION_NAME)
	static class RootDocumentClass {

		@Id String id;
		String firstname;
	}

	@Data
	static class ReturnTypeClass {

		@Field("firstname") String name;
	}

}
