/*
 * Copyright 2025-present the original author or authors.
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
package org.springframework.data.mongodb.core.mapping.event;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.auditing.ReactiveIsNewAwareAuditingHandler;
import org.springframework.data.mapping.context.PersistentEntities;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

/**
 * Unit tests for {@link ReactiveAuditingEntityCallback}.
 *
 * @author Mark Paluch
 */
class ReactiveAuditingEntityCallbackUnitTests {

	private final MongoMappingContext mappingContext = new MongoMappingContext();

	private ReactiveIsNewAwareAuditingHandler handler;
	private ReactiveAuditingEntityCallback callback;

	@BeforeEach
	void setUp() {

		mappingContext.getPersistentEntity(AuditingEntityCallbackUnitTests.Sample.class);

		handler = spy(new ReactiveIsNewAwareAuditingHandler(PersistentEntities.of(mappingContext)));

		callback = new ReactiveAuditingEntityCallback(() -> handler);
	}

	@Test // GH-4914
	void allowsChangingOrderDynamically() {

		callback.setOrder(50);
		assertThat(callback.getOrder()).isEqualTo(50);
	}
}
