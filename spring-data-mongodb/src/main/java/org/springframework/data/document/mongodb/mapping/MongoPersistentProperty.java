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

import org.springframework.data.mapping.PersistentProperty;

/**
 * Mongo specific {@link org.springframework.data.mapping.PersistentProperty} implementation.
 * 
 * @author Oliver Gierke
 */
public interface MongoPersistentProperty extends PersistentProperty<MongoPersistentProperty> {
	
	/**
	 * Returns the name of the field a property is persisted to.
	 * 
	 * @return
	 */
	String getFieldName();

	/**
	 * Returns whether the propert is a {@link com.mongodb.DBRef}. If this returns {@literal true} you can expect
	 * {@link #getDBRef()} to return an non-{@literal null} value.
	 * 
	 * @return
	 */
	boolean isDbReference();
	
	/**
	 * Returns the {@link DBRef} if the property is a reference.
	 * 
	 * @see #isDbReference()
	 * @return
	 */
	DBRef getDBRef();
}
