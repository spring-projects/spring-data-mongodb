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

package org.springframework.data.document.mongodb.index;

/**
 * @author Jon Brisbin <jbrisbin@vmware.com>
 */
public abstract class IndexPredicate {

	private String name;
	private IndexDirection direction = IndexDirection.ASCENDING;
	private boolean unique = false;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public IndexDirection getDirection() {
		return direction;
	}

	public void setDirection(IndexDirection direction) {
		this.direction = direction;
	}

	public boolean isUnique() {
		return unique;
	}

	public void setUnique(boolean unique) {
		this.unique = unique;
	}
}
