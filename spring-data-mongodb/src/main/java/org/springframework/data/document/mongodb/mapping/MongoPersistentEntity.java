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

package org.springframework.data.document.mongodb.mapping;

import org.springframework.data.mapping.model.AbstractPersistentEntity;
import org.springframework.data.mapping.model.ClassMapping;
import org.springframework.data.mapping.model.MappingContext;

/**
 * @author Jon Brisbin <jbrisbin@vmware.com>
 */
public class MongoPersistentEntity extends AbstractPersistentEntity {

  @SuppressWarnings({"unchecked"})
  public MongoPersistentEntity(Class<?> javaClass, MappingContext context) {
    super(javaClass, context);
  }

  @SuppressWarnings({"unchecked"})
  public ClassMapping<MongoCollection> getMapping() {
    return new MongoClassMapping(this, getMappingContext());
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    super.afterPropertiesSet();
  }

}
