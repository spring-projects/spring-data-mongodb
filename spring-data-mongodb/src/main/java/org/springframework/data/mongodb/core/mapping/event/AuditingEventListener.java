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

import org.springframework.context.ApplicationListener;
import org.springframework.data.auditing.AuditingHandler;
import org.springframework.data.auditing.IsNewAwareAuditingHandler;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.util.Assert;

/**
 * Event listener to populate auditing related fields on an entity about to be saved.
 * 
 * @author Oliver Gierke
 */
public class AuditingEventListener implements ApplicationListener<BeforeConvertEvent<Object>> {

	private final IsNewAwareAuditingHandler auditingHandler;

	/**
	 * Creates a new {@link AuditingEventListener} using the given {@link MappingContext} and {@link AuditingHandler}.
	 * 
	 * @param auditingHandler must not be {@literal null}.
	 */
	public AuditingEventListener(IsNewAwareAuditingHandler auditingHandler) {

		Assert.notNull(auditingHandler, "IsNewAwareAuditingHandler must not be null!");
		this.auditingHandler = auditingHandler;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.context.ApplicationListener#onApplicationEvent(org.springframework.context.ApplicationEvent)
	 */
	public void onApplicationEvent(BeforeConvertEvent<Object> event) {

		Object entity = event.getSource();
		auditingHandler.markAudited(entity);
	}
}
