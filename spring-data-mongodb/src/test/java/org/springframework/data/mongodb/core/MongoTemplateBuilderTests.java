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

import static org.springframework.data.mongodb.core.MongoTemplateBuilder.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;
import static org.springframework.data.mongodb.core.query.Update.*;

import lombok.Data;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Field;

import com.mongodb.MongoClient;

/**
 * @author Christoph Strobl
 * @since 2016/12
 */
public class MongoTemplateBuilderTests {

	private static final String COLLECTION_NAME = "collection-1";
	MongoTemplate template;

	@Before
	public void setUp() {

		template = new MongoTemplate(new SimpleMongoDbFactory(new MongoClient(), "test"));

		RootTypeClass c1 = new RootTypeClass();
		c1.firstname = "han";
		c1.id = "id-1";

		template.save(c1);
	}

	@Test
	public void allMethods() {

		// -- FINDERS
		find(query(where("firstname").is("han"))).inCollectionOf(RootTypeClass.class).execute();
		find(query(where("firstname").is("han"))).inCollectionOf(RootTypeClass.class).mappingTo(ReturnTypeClass.class).execute();
		find(query(where("firstname").is("han"))).in(COLLECTION_NAME).of(ReturnTypeClass.class).execute();
		find(query(where("firstname").is("han"))).in(COLLECTION_NAME).of(RootTypeClass.class).mappingTo(ReturnTypeClass.class).execute();

		// -- UPDATES
		find(query(where("firstname").is("han"))).inCollectionOf(RootTypeClass.class).andApply(update("firstname", "hunts alone")).execute();
	}

	@Data
	@org.springframework.data.mongodb.core.mapping.Document(collection = COLLECTION_NAME)
	static class RootTypeClass {

		@Id String id;
		String firstname;
	}

	@Data
	static class ReturnTypeClass {

		@Field("firstname") String name;
	}

}
