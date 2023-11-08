/*
 * Copyright 2023 the original author or authors.
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
package org.springframework.data.mongodb.core;

import static org.assertj.core.api.AssertionsForClassTypes.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.data.domain.KeysetScrollPosition;
import org.springframework.data.domain.ScrollPosition;
import org.springframework.data.domain.Window;
import org.springframework.data.mongodb.core.EntityOperations.Entity;
import org.springframework.data.mongodb.core.query.Query;

/**
 * Unit tests for {@link ScrollUtils}.
 *
 * @author Mark Paluch
 */
class ScrollUtilsUnitTests {

	@Test // GH-4413
	void positionShouldRetainScrollDirection() {

		Query query = new Query();
		query.with(ScrollPosition.keyset().backward());
		EntityOperations entityOperationsMock = mock(EntityOperations.class);
		Entity entityMock = mock(Entity.class);

		when(entityOperationsMock.forEntity(any())).thenReturn(entityMock);
		when(entityMock.extractKeys(any(), any())).thenReturn(Map.of("k", "v"));

		Window<Integer> window = ScrollUtils.createWindow(query, new ArrayList<>(List.of(1, 2, 3)), Integer.class,
				entityOperationsMock);

		assertThat(window.positionAt(0)).isInstanceOf(KeysetScrollPosition.class);
		assertThat(((KeysetScrollPosition) window.positionAt(0)).scrollsBackward()).isTrue();
	}
}
