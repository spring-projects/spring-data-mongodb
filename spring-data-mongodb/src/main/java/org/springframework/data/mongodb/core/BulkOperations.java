/*
 * Copyright 2015 the original author or authors.
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

import java.util.List;

import org.springframework.data.mongodb.BulkOperationException;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.util.Tuple;

import com.mongodb.BulkWriteResult;

/**
 * Bulk operations for insert/update/remove actions on a collection.
 * <p/>
 * These bulks operation are available since MongoDB 2.6 and make use of low level bulk commands on the protocol level.
 * 
 * @author Tobias Trelle
 */
public interface BulkOperations {

	/** Mode for bulk operation. */
	public enum BulkMode {

		/** Perform bulk operations in sequence. The first error will cancel processing. */
		ORDERED,

		/** Perform bulk operations in parallel. Processing will continue on errors. */
		UNORDERED
	};

	/**
	 * Bulk insert operation.
	 * 
	 * @param mode Bulk mode
	 * @param documents List of documents to insert in a bulk.
	 * 
	 * @return Number of inserted documents.
	 * 
	 * @throws BulkOperationException if an error occured during bulk processing.
	 */
	int insert(BulkMode mode, List<? extends Object> documents);

	/**
	 * Process a bulk of update requests. For each update request, only the first matching
	 * document is updated.
	 * 
	 * @param mode
	 * @param updates
	 * @param writeConcern
	 * @return
	 */
	int updateOne(BulkMode mode, List<Tuple<Query, Update>> updates);

	int updateMulti(BulkMode mode, List<Tuple<Query, Update>> updates);
	
	BulkWriteResult upsert(BulkMode mode, List<Tuple<Query, Update>> updates);
	
	int remove(BulkMode mode, List<Query> removes);
	
}
