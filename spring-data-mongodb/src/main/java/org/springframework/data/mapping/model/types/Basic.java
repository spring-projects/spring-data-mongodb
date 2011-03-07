/* Copyright (C) 2011 SpringSource
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

import java.beans.PropertyDescriptor;

import org.springframework.data.mapping.model.MappingContext;
import org.springframework.data.mapping.model.PersistentEntity;
import org.springframework.data.mapping.model.PropertyMapping;

/**
 * Models a basic collection type such as a list of Strings
 * 
 * @author Graeme Rocher
 * @since 1.0
 *
 */
public abstract class Basic extends Association {

	public Basic(PersistentEntity owner, MappingContext context,
			PropertyDescriptor descriptor) {
		super(owner, context, descriptor);
	}

	public Basic(PersistentEntity owner, MappingContext context, String name, Class type) {
		super(owner, context, name, type);

	}

	@Override
	public Association getInverseSide() {
		return null; // basic collection types have no inverse side
	}

	@Override
	public boolean isOwningSide() {
		return true;
	}

	@Override
	public void setOwningSide(boolean owningSide) {
		// noop
	}

	@Override
	public PersistentEntity getAssociatedEntity() {
		return null; // basic collection types have no associated entity
	}
	
	
}
