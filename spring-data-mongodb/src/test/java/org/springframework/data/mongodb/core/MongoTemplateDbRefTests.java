/*
 * Copyright 2017 the original author or authors.
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

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import lombok.Data;

import java.net.UnknownHostException;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.Document;

import com.mongodb.MongoClient;

/**
 * {@link org.springframework.data.mongodb.core.mapping.DBRef} related integration tests for
 * {@link org.springframework.data.mongodb.core.MongoTemplate}.
 *
 * @author Christoph Strobl
 */
public class MongoTemplateDbRefTests {

	MongoTemplate template;

	@Before
	public void setUp() throws UnknownHostException {

		template = new MongoTemplate(new MongoClient(), "mongo-template-dbref-tests");

		template.dropCollection(RefCycleLoadingIntoDifferentTypeRoot.class);
		template.dropCollection(RefCycleLoadingIntoDifferentTypeIntermediate.class);
		template.dropCollection(RefCycleLoadingIntoDifferentTypeRootView.class);
	}

	@Test // DATAMONGO-1703
	public void shouldLoadRefIntoDifferentTypeCorrectly() {

		// init root
		RefCycleLoadingIntoDifferentTypeRoot root = new RefCycleLoadingIntoDifferentTypeRoot();
		root.id = "root-1";
		root.content = "jon snow";
		template.save(root);

		// init one and set view id ref to root.id
		RefCycleLoadingIntoDifferentTypeIntermediate intermediate = new RefCycleLoadingIntoDifferentTypeIntermediate();
		intermediate.id = "one-1";
		intermediate.refToRootView = new RefCycleLoadingIntoDifferentTypeRootView();
		intermediate.refToRootView.id = root.id;

		template.save(intermediate);

		// add one ref to root
		root.refToIntermediate = intermediate;
		template.save(root);

		RefCycleLoadingIntoDifferentTypeRoot loaded = template.findOne(query(where("id").is(root.id)),
				RefCycleLoadingIntoDifferentTypeRoot.class);

		assertThat(loaded.content, is(equalTo("jon snow")));
		assertThat(loaded.getRefToIntermediate(), is(instanceOf(RefCycleLoadingIntoDifferentTypeIntermediate.class)));
		assertThat(loaded.getRefToIntermediate().getRefToRootView(),
				is(instanceOf(RefCycleLoadingIntoDifferentTypeRootView.class)));
		assertThat(loaded.getRefToIntermediate().getRefToRootView().getContent(), is(equalTo("jon snow")));
	}

	@Data
	@Document(collection = "cycle-with-different-type-root")
	static class RefCycleLoadingIntoDifferentTypeRoot {

		@Id String id;
		String content;
		@DBRef RefCycleLoadingIntoDifferentTypeIntermediate refToIntermediate;
	}

	@Data
	@Document(collection = "cycle-with-different-type-intermediate")
	static class RefCycleLoadingIntoDifferentTypeIntermediate {

		@Id String id;
		@DBRef RefCycleLoadingIntoDifferentTypeRootView refToRootView;
	}

	@Data
	@Document(collection = "cycle-with-different-type-root")
	static class RefCycleLoadingIntoDifferentTypeRootView {

		@Id String id;
		String content;
	}

}
