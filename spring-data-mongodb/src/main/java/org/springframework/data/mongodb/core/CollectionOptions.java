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

/**
 * Provides a simple wrapper to encapsulate the variety of settings you can use when creating a collection.
 * 
 * @author Thomas Risberg
 */
public class CollectionOptions {

	private Integer maxDocuments;

	private Integer size;

	private Boolean capped;

	/**
	 * Constructs a new <code>CollectionOptions</code> instance.
	 * 
	 * @param size the collection size in bytes, this data space is preallocated
	 * @param maxDocuments the maximum number of documents in the collection.
	 * @param capped true to created a "capped" collection (fixed size with auto-FIFO behavior based on insertion order),
	 *          false otherwise.
	 */
	public CollectionOptions(Integer size, Integer maxDocuments, Boolean capped) {
		super();
		this.maxDocuments = maxDocuments;
		this.size = size;
		this.capped = capped;
	}

	public Integer getMaxDocuments() {
		return maxDocuments;
	}

	public void setMaxDocuments(Integer maxDocuments) {
		this.maxDocuments = maxDocuments;
	}

	public Integer getSize() {
		return size;
	}

	public void setSize(Integer size) {
		this.size = size;
	}

	public Boolean getCapped() {
		return capped;
	}

	public void setCapped(Boolean capped) {
		this.capped = capped;
	}

}
