/*
 * Copyright 2026-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.springframework.dao.DataAccessException;
import org.springframework.data.mongodb.core.MongoTemplate.SourceAwareDocument;
import org.springframework.data.mongodb.core.QueryOperations.DeleteContext;
import org.springframework.data.mongodb.core.QueryOperations.UpdateContext;
import org.springframework.data.mongodb.core.bulk.Bulk;
import org.springframework.data.mongodb.core.bulk.BulkWriteOptions;
import org.springframework.data.mongodb.core.bulk.BulkOperation;
import org.springframework.data.mongodb.core.bulk.BulkOperation.Insert;
import org.springframework.data.mongodb.core.bulk.BulkOperation.Remove;
import org.springframework.data.mongodb.core.bulk.BulkOperation.RemoveFirst;
import org.springframework.data.mongodb.core.bulk.BulkOperation.Replace;
import org.springframework.data.mongodb.core.bulk.BulkOperation.Update;
import org.springframework.data.mongodb.core.bulk.BulkOperation.UpdateFirst;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.event.AfterSaveEvent;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoNamespace;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.bulk.ClientBulkWriteOptions;
import com.mongodb.client.model.bulk.ClientBulkWriteResult;
import com.mongodb.client.model.bulk.ClientNamespacedWriteModel;

/**
 * Internal API wrapping a {@link MongoTemplate} to encapsulate {@link Bulk} handling.
 * 
 * @author Christoph Strobl
 * @since 2026/02
 */
class BulkWriter {

	MongoTemplate template;

	BulkWriter(MongoTemplate template) {
		this.template = template;
	}

	public ClientBulkWriteResult write(String defaultDatabase, Bulk bulk, BulkWriteOptions options) {

		List<ClientNamespacedWriteModel> writeModels = new ArrayList<>();
		List<SourceAwareDocument<Object>> afterSaveCallables = new ArrayList<>();

		for (BulkOperation bulkOp : bulk.operations()) {

			String collectionName = bulkOp.context().namespace().collection() != null
					? bulkOp.context().namespace().collection()
					: template.getCollectionName(bulkOp.context().namespace().type());

			MongoNamespace mongoNamespace = new MongoNamespace(defaultDatabase, collectionName);
			if (bulkOp instanceof Insert insert) {

				SourceAwareDocument<Object> sourceAwareDocument = template.prepareObjectForSave(collectionName, insert.value(),
						template.getConverter());
				writeModels.add(ClientNamespacedWriteModel.insertOne(mongoNamespace, sourceAwareDocument.document()));
				afterSaveCallables.add(sourceAwareDocument);
			} else if (bulkOp instanceof Update update) {

				Class<?> domainType = update.context().namespace().type();
				boolean multi = !(bulkOp instanceof UpdateFirst);

				UpdateContext updateContext = template.getQueryOperations().updateContext(update.update(), update.query(),
						update.upsert());
				MongoPersistentEntity<?> entity = template.getPersistentEntity(domainType);

				Document mappedQuery = updateContext.getMappedQuery(entity);
				Object mappedUpdate = updateContext.isAggregationUpdate() ? updateContext.getUpdatePipeline(domainType)
						: updateContext.getMappedUpdate(entity);
				UpdateOptions updateOptions = updateContext.getUpdateOptions(domainType, update.query());

				if (multi) {
					writeModels.add(BulkWriteSupport.updateMany(mongoNamespace, mappedQuery, mappedUpdate, updateOptions));
				} else {
					writeModels.add(BulkWriteSupport.updateOne(mongoNamespace, mappedQuery, mappedUpdate, updateOptions));
				}
			} else if (bulkOp instanceof Remove remove) {

				Class<?> domainType = remove.context().namespace().type();
				DeleteContext deleteContext = template.getQueryOperations().deleteQueryContext(remove.query());

				Document mappedQuery = deleteContext.getMappedQuery(template.getPersistentEntity(domainType));
				DeleteOptions deleteOptions = deleteContext.getDeleteOptions(domainType);

				if (remove instanceof RemoveFirst) {
					writeModels.add(BulkWriteSupport.removeOne(mongoNamespace, mappedQuery, deleteOptions));
				} else {
					writeModels.add(BulkWriteSupport.removeMany(mongoNamespace, mappedQuery, deleteOptions));
				}
			} else if (bulkOp instanceof Replace replace) {

				Class<?> domainType = replace.context().namespace().type();

				SourceAwareDocument<Object> sourceAwareDocument = template.prepareObjectForSave(collectionName,
						replace.replacement(), template.getConverter());

				UpdateContext updateContext = template.getQueryOperations().replaceSingleContext(replace.query(),
						MappedDocument.of(sourceAwareDocument.document()), replace.upsert());

				Document mappedQuery = updateContext.getMappedQuery(template.getPersistentEntity(domainType));
				UpdateOptions updateOptions = updateContext.getUpdateOptions(domainType, replace.query());

				writeModels.add(
						BulkWriteSupport.replaceOne(mongoNamespace, mappedQuery, sourceAwareDocument.document(), updateOptions));
				afterSaveCallables.add(sourceAwareDocument);
			}
		}

		try {

			ClientBulkWriteResult clientBulkWriteResult = template.doWithClient(client -> client.bulkWrite(writeModels,
					ClientBulkWriteOptions.clientBulkWriteOptions().ordered(options.getOrder().equals(BulkWriteOptions.Order.ORDERED))));

			afterSaveCallables.forEach(callable -> {
				template
						.maybeEmitEvent(new AfterSaveEvent<>(callable.source(), callable.document(), callable.collectionName()));
				template.maybeCallAfterSave(callable.source(), callable.document(), callable.collectionName());
			});
			return clientBulkWriteResult;
		} catch (MongoBulkWriteException e) {
			DataAccessException dataAccessException = template.getExceptionTranslator().translateExceptionIfPossible(e);
			if (dataAccessException != null) {
				throw dataAccessException;
			}
			throw e;
		}
	}
}
