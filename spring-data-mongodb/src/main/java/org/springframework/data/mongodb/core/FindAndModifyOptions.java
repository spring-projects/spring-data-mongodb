/*
 * Copyright 2010-2011 the original author or authors.
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

public class FindAndModifyOptions {

	boolean returnNew;

	boolean upsert;

	boolean remove;

	/**
	 * Static factory method to create a FindAndModifyOptions instance
	 * 
	 * @return a new instance
	 */
	public static FindAndModifyOptions options() {
		return new FindAndModifyOptions();
	}

	public FindAndModifyOptions returnNew(boolean returnNew) {
		this.returnNew = returnNew;
		return this;
	}

	public FindAndModifyOptions upsert(boolean upsert) {
		this.upsert = upsert;
		return this;
	}

	public FindAndModifyOptions remove(boolean remove) {
		this.remove = remove;
		return this;
	}

	public boolean isReturnNew() {
		return returnNew;
	}

	public boolean isUpsert() {
		return upsert;
	}

	public boolean isRemove() {
		return remove;
	}

}
