/*
 * Copyright 2017-2018 the original author or authors.
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
package org.springframework.data.mongodb.repository.support;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.mongodb.core.index.IndexOperationsProvider;
import org.springframework.data.mongodb.repository.query.PartTreeMongoQuery;
import org.springframework.data.repository.query.parser.PartTree;

/**
 * Unit tests for {@link IndexEnsuringQueryCreationListener}.
 *
 * @author Oliver Gierke
 */
@RunWith(MockitoJUnitRunner.class)
public class IndexEnsuringQueryCreationListenerUnitTests {

	IndexEnsuringQueryCreationListener listener;

	@Mock IndexOperationsProvider provider;

	@Before
	public void setUp() {
		this.listener = new IndexEnsuringQueryCreationListener(provider);
	}

	@Test // DATAMONGO-1753
	public void skipsQueryCreationForMethodWithoutPredicate() {

		PartTree tree = mock(PartTree.class);
		when(tree.hasPredicate()).thenReturn(false);

		PartTreeMongoQuery query = mock(PartTreeMongoQuery.class, Answers.RETURNS_MOCKS);
		when(query.getTree()).thenReturn(tree);

		listener.onCreation(query);

		verify(provider, times(0)).indexOps(any());
	}

	interface SampleRepository {

		Object findAllBy();
	}
}
