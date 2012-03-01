/*
 * Copyright 2012 the original author or authors.
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
package org.springframework.data.mongodb.config;

import org.springframework.core.convert.converter.Converter;

import com.mongodb.WriteConcern;

/**
 * Converter to create {@link WriteConcern} instances from String representations.
 * 
 * @author Oliver Gierke
 */
public class StringToWriteConcernConverter implements Converter<String, WriteConcern> {

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
	 */
	public WriteConcern convert(String source) {

		WriteConcern writeConcern = WriteConcern.valueOf(source);
		return writeConcern != null ? writeConcern : new WriteConcern(source);
	}
}
