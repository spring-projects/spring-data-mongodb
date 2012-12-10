/*
 * Copyright 2011-2012 the original author or authors.
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
package org.springframework.data.mongodb.core;

import com.mongodb.WriteConcern;

/**
 * A strategy interface to determine the {@link WriteConcern} to use for a given {@link MongoAction}. Return the passed
 * in default {@link WriteConcern} (a property on {@link MongoAction}) if no determination can be made.
 * 
 * @author Mark Pollack
 * @author Oliver Gierke
 */
public interface WriteConcernResolver {

	/**
	 * Resolve the {@link WriteConcern} given the {@link MongoAction}.
	 * 
	 * @param action describes the context of the Mongo action. Contains a default {@link WriteConcern} to use if one
	 *          should not be resolved.
	 * @return a {@link WriteConcern} based on the passed in {@link MongoAction} value, maybe {@literal null}.
	 */
	WriteConcern resolve(MongoAction action);
}
