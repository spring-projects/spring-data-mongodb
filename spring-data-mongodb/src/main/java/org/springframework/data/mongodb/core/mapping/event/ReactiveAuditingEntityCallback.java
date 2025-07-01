/*
 * Copyright 2019-2025 the original author or authors.
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

import org.reactivestreams.Publisher;

import org.springframework.beans.factory.ObjectFactory;
import org.springframework.core.Ordered;
import org.springframework.data.auditing.AuditingHandler;
import org.springframework.data.auditing.ReactiveIsNewAwareAuditingHandler;
import org.springframework.data.mapping.callback.EntityCallback;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.util.Assert;

/**
 * Reactive {@link EntityCallback} to populate auditing related fields on an entity about to be saved.
 *
 * @author Mark Paluch
 * @since 2.2
 */
public class ReactiveAuditingEntityCallback implements ReactiveBeforeConvertCallback<Object>, Ordered {

	private final ObjectFactory<ReactiveIsNewAwareAuditingHandler> auditingHandlerFactory;
	private int order = 100;

	/**
	 * Creates a new {@link ReactiveAuditingEntityCallback} using the given {@link MappingContext} and
	 * {@link AuditingHandler} provided by the given {@link ObjectFactory}.
	 *
	 * @param auditingHandlerFactory must not be {@literal null}.
	 */
	public ReactiveAuditingEntityCallback(ObjectFactory<ReactiveIsNewAwareAuditingHandler> auditingHandlerFactory) {

		Assert.notNull(auditingHandlerFactory, "IsNewAwareAuditingHandler must not be null");
		this.auditingHandlerFactory = auditingHandlerFactory;
	}

	@Override
	public int getOrder() {
		return this.order;
	}

	/**
	 * Specify the order value for this {@link BeforeConvertCallback}.
	 * <p>
	 * The default value is {@code 100}.
	 *
	 * @see org.springframework.core.Ordered#getOrder()
	 * @since 5.0
	 */
	public void setOrder(int order) {
		this.order = order;
	}

	@Override
	public Publisher<Object> onBeforeConvert(Object entity, String collection) {
		return auditingHandlerFactory.getObject().markAudited(entity);
	}
}
