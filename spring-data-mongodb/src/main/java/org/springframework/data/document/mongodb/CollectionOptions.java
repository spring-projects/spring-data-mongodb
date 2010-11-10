/*
 * Copyright 2010 the original author or authors.
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
package org.springframework.data.document.mongodb;

public class CollectionOptions {
	
	private Integer maxDocuments;
	
	private Integer size;
	
	private Boolean capped;
	
	
	
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
