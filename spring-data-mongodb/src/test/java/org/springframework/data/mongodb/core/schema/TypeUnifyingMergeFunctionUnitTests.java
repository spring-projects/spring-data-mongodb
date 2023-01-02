/*
 * Copyright 2022-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.schema;

import static org.mockito.Mockito.*;
import static org.springframework.data.mongodb.test.util.Assertions.*;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.schema.MongoJsonSchema.ConflictResolutionFunction;
import org.springframework.data.mongodb.core.schema.MongoJsonSchema.ConflictResolutionFunction.Resolution;

/**
 * Unit tests for {@link TypeUnifyingMergeFunction}.
 *
 * @author Christoph Strobl
 */
@ExtendWith(MockitoExtension.class)
public class TypeUnifyingMergeFunctionUnitTests {

	@Mock ConflictResolutionFunction crf;

	TypeUnifyingMergeFunction mergeFunction;

	@BeforeEach
	void beforeEach() {
		mergeFunction = new TypeUnifyingMergeFunction(crf);
	}

	@Test // GH-3870
	void nonOverlapping() {

		Map<String, Object> a = new LinkedHashMap<>();
		a.put("a", "a-value");
		Map<String, Object> b = new LinkedHashMap<>();
		b.put("b", "b-value");

		Document target = mergeFunction.apply(a, b);
		assertThat(target).containsEntry("a", "a-value").containsEntry("b", "b-value");
	}

	@Test // GH-3870
	void resolvesNonConflictingTypeKeys/* type vs bsonType */() {

		Map<String, Object> a = new LinkedHashMap<>();
		a.put("type", "string");
		Map<String, Object> b = new LinkedHashMap<>();
		b.put("bsonType", "string");

		Document target = mergeFunction.apply(a, b);
		assertThat(target).containsEntry("type", "string").doesNotContainKey("bsonType");
	}

	@Test // GH-3870
	void nonOverlappingNestedMap() {

		Map<String, Object> a = new LinkedHashMap<>();
		a.put("a", Collections.singletonMap("nested", "value"));
		Map<String, Object> b = new LinkedHashMap<>();
		b.put("b", "b-value");

		Document target = mergeFunction.apply(a, b);
		assertThat(target).containsEntry("a", Collections.singletonMap("nested", "value")).containsEntry("b", "b-value");
	}

	@Test // GH-3870
	void nonOverlappingNestedMaps() {

		Map<String, Object> a = new LinkedHashMap<>();
		a.put("nested", Collections.singletonMap("a", "a-value"));
		Map<String, Object> b = new LinkedHashMap<>();
		b.put("nested", Collections.singletonMap("b", "b-value"));

		Document target = mergeFunction.apply(a, b);
		assertThat(target).containsEntry("nested.a", "a-value").containsEntry("nested.b", "b-value");
	}

	@Test // GH-3870
	void delegatesConflictToResolutionFunction() {

		ArgumentCaptor<Object> aValueCaptor = ArgumentCaptor.forClass(Object.class);
		ArgumentCaptor<Object> bValueCaptor = ArgumentCaptor.forClass(Object.class);

		when(crf.resolveConflict(any(), aValueCaptor.capture(), bValueCaptor.capture())).thenReturn(Resolution.ofValue("nested", "from-function"));

		Map<String, Object> a = new LinkedHashMap<>();
		a.put("nested", Collections.singletonMap("a", "a-value"));
		Map<String, Object> b = new LinkedHashMap<>();
		b.put("nested", "b-value");

		Document target = mergeFunction.apply(a, b);
		assertThat(target).containsEntry("nested", "from-function") //
				.doesNotContainKey("nested.a");

		assertThat(aValueCaptor.getValue()).isEqualTo(a);
		assertThat(bValueCaptor.getValue()).isEqualTo(b);
	}

	@Test // GH-3870
	void skipsConflictItemsWhenAdvised() {

		ArgumentCaptor<Object> aValueCaptor = ArgumentCaptor.forClass(Object.class);
		ArgumentCaptor<Object> bValueCaptor = ArgumentCaptor.forClass(Object.class);

		when(crf.resolveConflict(any(), aValueCaptor.capture(), bValueCaptor.capture())).thenReturn(Resolution.SKIP);

		Map<String, Object> a = new LinkedHashMap<>();
		a.put("nested", Collections.singletonMap("a", "a-value"));
		a.put("some", "value");
		Map<String, Object> b = new LinkedHashMap<>();
		b.put("nested", "b-value");

		Document target = mergeFunction.apply(a, b);
		assertThat(target).hasSize(1).containsEntry("some", "value");
	}
}
