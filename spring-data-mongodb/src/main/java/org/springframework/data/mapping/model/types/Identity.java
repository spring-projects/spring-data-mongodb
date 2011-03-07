/* Copyright 2004-2005 the original author or authors.
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
package org.springframework.data.mapping.model.types;

import org.springframework.data.mapping.model.AbstractPersistentProperty;
import org.springframework.data.mapping.model.MappingContext;
import org.springframework.data.mapping.model.PersistentEntity;

import java.beans.PropertyDescriptor;

/**
 * Represents the identity of a persistent entity
 * 
 * @author Graeme Rocher
 * @since 1.0
 */
public abstract class Identity<T> extends AbstractPersistentProperty {
    public Identity(PersistentEntity owner, MappingContext context, PropertyDescriptor descriptor) {
        super(owner, context, descriptor);
    }
    public Identity(PersistentEntity owner, MappingContext context, String name, Class type) {
        super(owner, context, name, type);
    }
}
