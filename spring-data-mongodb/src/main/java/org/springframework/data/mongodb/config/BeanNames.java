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

package org.springframework.data.mongodb.config;

/**
 * @author Jon Brisbin <jbrisbin@vmware.com>
 */
public abstract class BeanNames {

	static final String MAPPING_CONTEXT = "mappingContext";
	static final String INDEX_HELPER = "indexCreationHelper";
	static final String MONGO = "mongo";
	static final String DB_FACTORY = "mongoDbFactory";
	static final String VALIDATING_EVENT_LISTENER = "validatingMongoEventListener";
	static final String IS_NEW_STRATEGY_FACTORY = "isNewStrategyFactory";
	static final String DEFAULT_CONVERTER_BEAN_NAME = "mappingConverter";
	// TODO or mongoOperations?
	static final String MONGO_TEMPLATE = "mongoTemplate";
	// TODO or gridFsOperations?
	static final String GRID_FS_TEMPLATE = "gridFsTemplate";
}
