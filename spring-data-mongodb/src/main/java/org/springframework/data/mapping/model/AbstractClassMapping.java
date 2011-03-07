/* Copyright (C) 2010 SpringSource
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
package org.springframework.data.mapping.model;

/**
 * Abstract implementation of the ClassMapping interface
 *
 * @author Graeme Rocher
 * @since 1.0
 */
public abstract class AbstractClassMapping<T> implements ClassMapping{
    protected PersistentEntity entity;
    protected MappingContext context;

    public AbstractClassMapping(PersistentEntity entity, MappingContext context) {
        super();
        this.entity = entity;
        this.context = context;
    }

    public PersistentEntity getEntity() {
        return this.entity;
    }

    public abstract T getMappedForm();

    public IdentityMapping getIdentifier() {
        return context.getMappingSyntaxStrategy().getDefaultIdentityMapping(this);
    }
}
