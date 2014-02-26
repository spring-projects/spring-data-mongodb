/*
 * Copyright 2011-2014 the original author or authors.
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
package org.springframework.data.mongodb.core.geo;

import java.util.List;

/**
 * Common interface for all shapes. Allows building MongoDB representations of them.
 * 
 * @author Oliver Gierke
 * @author Thomas Darimont
 */
@Deprecated
public interface Shape extends org.springframework.data.geo.Shape {

	/**
	 * Returns the {@link Shape} as a list of usually {@link Double} or {@link List}s of {@link Double}s. Wildcard bound
	 * to allow implementations to return a more concrete element type.
	 * 
	 * @return
	 */
	List<? extends Object> asList();

	/**
	 * Returns the command to be used to create the {@literal $within} criterion.
	 * 
	 * @return
	 */
	String getCommand();
}
