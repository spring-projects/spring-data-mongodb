/*
 * Copyright 2011-2017 the original author or authors.
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
package org.springframework.data.mongodb.gridfs;

import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.lang.Nullable;

/**
 * GridFs-specific helper class to define {@link Criteria}s.
 * 
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
public class GridFsCriteria extends Criteria {

	/**
	 * Creates a new {@link GridFsCriteria} for the given key.
	 * 
	 * @param key
	 */
	public GridFsCriteria(String key) {
		super(key);
	}

	/**
	 * Creates a {@link GridFsCriteria} for restrictions on the file's metadata.
	 * 
	 * @return
	 */
	public static GridFsCriteria whereMetaData() {
		return new GridFsCriteria("metadata");
	}

	/**
	 * Creates a {@link GridFsCriteria} for restrictions on a single file's metadata item.
	 * 
	 * @param metadataKey
	 * @return
	 */
	public static GridFsCriteria whereMetaData(@Nullable String metadataKey) {

		String extension = metadataKey == null ? "" : "." + metadataKey;
		return new GridFsCriteria(String.format("metadata%s", extension));
	}

	/**
	 * Creates a {@link GridFsCriteria} for restrictions on the file's name.
	 * 
	 * @return
	 */
	public static GridFsCriteria whereFilename() {
		return new GridFsCriteria("filename");
	}

	/**
	 * Creates a {@link GridFsCriteria} for restrictions on the file's content type.
	 * 
	 * @return
	 */
	public static GridFsCriteria whereContentType() {
		return new GridFsCriteria("metadata.".concat(GridFsResource.CONTENT_TYPE_FIELD));
	}
}
