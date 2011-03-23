/*
 * Copyright (c) 2011 by the original author(s).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.document.mongodb.mapping.event;

import org.springframework.context.ApplicationEvent;
import org.springframework.data.document.mongodb.event.EventType;
import org.springframework.data.mapping.model.PersistentEntity;

/**
 * @author Jon Brisbin <jbrisbin@vmware.com>
 */
public class MongoMappingEvent<T> extends ApplicationEvent {
  
  private static final long serialVersionUID = 1L;
  private final EventType type;
  private final PersistentEntity<T> entity;

  public MongoMappingEvent(EventType type, PersistentEntity<T> entity, T target) {
    super(target);
    this.type = type;
    this.entity = entity;
  }

  public EventType getType() {
    return type;
  }

  public PersistentEntity<T> getEntity() {
    return entity;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T getSource() {
    return (T) super.getSource();
  }
}
