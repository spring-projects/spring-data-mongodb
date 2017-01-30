/*
 * Copyright 2012-2014 the original author or authors.
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
package org.springframework.data.mongodb.core.mapping.event;

import java.util.Optional;

import org.springframework.beans.factory.ObjectFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.core.Ordered;
import org.springframework.data.auditing.AuditingHandler;
import org.springframework.data.auditing.IsNewAwareAuditingHandler;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.util.Assert;

/**
 * Event listener to populate auditing related fields on an entity about to be saved.
 * 
 * @author Oliver Gierke
 * @author Thomas Darimont
 */
public class AuditingEventListener implements ApplicationListener<BeforeConvertEvent<Object>>, Ordered {

	private final ObjectFactory<IsNewAwareAuditingHandler> auditingHandlerFactory;

	/**
	 * Creates a new {@link AuditingEventListener} using the given {@link MappingContext} and {@link AuditingHandler}
	 * provided by the given {@link ObjectFactory}.
	 * 
	 * @param auditingHandlerFactory must not be {@literal null}.
	 */
	public AuditingEventListener(ObjectFactory<IsNewAwareAuditingHandler> auditingHandlerFactory) {

		Assert.notNull(auditingHandlerFactory, "IsNewAwareAuditingHandler must not be null!");
		this.auditingHandlerFactory = auditingHandlerFactory;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.context.ApplicationListener#onApplicationEvent(org.springframework.context.ApplicationEvent)
	 */
	public void onApplicationEvent(BeforeConvertEvent<Object> event) {

		Object entity = event.getSource();
		auditingHandlerFactory.getObject().markAudited(Optional.ofNullable(entity));
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.core.Ordered#getOrder()
	 */
	@Override
	public int getOrder() {
		return 100;
	}
}
