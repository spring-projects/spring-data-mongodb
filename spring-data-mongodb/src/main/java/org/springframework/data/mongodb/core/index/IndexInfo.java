/*
 * Copyright 2002-2011 the original author or authors.
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
package org.springframework.data.mongodb.core.index;

import java.util.Map;

import org.springframework.data.mongodb.core.query.Order;

public class IndexInfo {

	private final Map<String, Order> fieldSpec;

	private String name;

	private boolean unique = false;

	private boolean dropDuplicates = false;

	private boolean sparse = false;


	public IndexInfo(Map<String, Order> fieldSpec, String name, boolean unique, boolean dropDuplicates, boolean sparse) {
		super();
		this.fieldSpec = fieldSpec;
		this.name = name;
		this.unique = unique;
		this.dropDuplicates = dropDuplicates;
		this.sparse = sparse;
	}

	public Map<String, Order> getFieldSpec() {
		return fieldSpec;
	}

	public String getName() {
		return name;
	}

	public boolean isUnique() {
		return unique;
	}

	public boolean isDropDuplicates() {
		return dropDuplicates;
	}

	public boolean isSparse() {
		return sparse;
	}

	@Override
	public String toString() {
		return "IndexInfo [fieldSpec=" + fieldSpec + ", name=" + name + ", unique=" + unique + ", dropDuplicates="
				+ dropDuplicates + ", sparse=" + sparse + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (dropDuplicates ? 1231 : 1237);
		result = prime * result + ((fieldSpec == null) ? 0 : fieldSpec.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + (sparse ? 1231 : 1237);
		result = prime * result + (unique ? 1231 : 1237);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		IndexInfo other = (IndexInfo) obj;
		if (dropDuplicates != other.dropDuplicates)
			return false;
		if (fieldSpec == null) {
			if (other.fieldSpec != null)
				return false;
		} else if (!fieldSpec.equals(other.fieldSpec))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (sparse != other.sparse)
			return false;
		if (unique != other.unique)
			return false;
		return true;
	}


	/**
	 * [{ "v" : 1 , "key" : { "_id" : 1} ,  "ns" : "database.person" , "name" : "_id_"}, 
      { "v" : 1 , "key" : { "age" : -1} , "ns" : "database.person" , "name" : "age_-1" ,  "unique" : true , "dropDups" : true}]
	 */
}
