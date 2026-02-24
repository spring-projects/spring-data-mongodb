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

import java.util.List;

import org.bson.Document;
import org.springframework.util.ClassUtils;

import com.mongodb.MongoNamespace;
import com.mongodb.client.model.DeleteManyModel;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.model.bulk.ClientDeleteManyOptions;
import com.mongodb.client.model.bulk.ClientDeleteOneOptions;
import com.mongodb.client.model.bulk.ClientNamespacedWriteModel;
import com.mongodb.client.model.bulk.ClientReplaceOneOptions;
import com.mongodb.client.model.bulk.ClientUpdateManyOptions;
import com.mongodb.client.model.bulk.ClientUpdateOneOptions;

/**
 * @author Christoph Strobl
 */
abstract class BulkWriteSupport {

	static WriteModel<Document> updateMany(Document query, Object update, UpdateOptions updateOptions) {

		if (update instanceof List<?> pipeline) {
			return new UpdateManyModel<>(query, (List<Document>) pipeline, updateOptions);
		} else if (update instanceof Document updateDocument) {
			return new UpdateManyModel<>(query, updateDocument, updateOptions);
		} else {
			throw new IllegalArgumentException(
					"Update needs to be either a List or a Document, but was [%s]".formatted(ClassUtils.getUserClass(update)));
		}
	}

	static WriteModel<Document> updateOne(Document query, Object update, UpdateOptions updateOptions) {

		if (update instanceof List<?> pipeline) {
			return new UpdateOneModel<>(query, (List<Document>) pipeline, updateOptions);
		} else if (update instanceof Document updateDocument) {
			return new UpdateOneModel<>(query, updateDocument, updateOptions);
		} else {
			throw new IllegalArgumentException(
					"Update needs to be either a List or a Document, but was [%s]".formatted(ClassUtils.getUserClass(update)));
		}
	}

	static WriteModel<Document> removeMany(Document query, DeleteOptions deleteOptions) {
		return new DeleteManyModel<>(query, deleteOptions);
	}

	static WriteModel<Document> removeOne(Document query, DeleteOptions deleteOptions) {
		return new DeleteOneModel<>(query, deleteOptions);
	}

	static WriteModel<Document> replaceOne(Document query, Document replacement, UpdateOptions updateOptions) {

		ReplaceOptions replaceOptions = new ReplaceOptions();
		replaceOptions.collation(updateOptions.getCollation());
		replaceOptions.upsert(updateOptions.isUpsert());
		replaceOptions.sort(updateOptions.getSort());
		replaceOptions.hint(updateOptions.getHint());
		replaceOptions.hintString(updateOptions.getHintString());

		return new ReplaceOneModel<>(query, replacement, replaceOptions);
	}

	static ClientNamespacedWriteModel updateMany(MongoNamespace namespace, Document query, Object update,
			UpdateOptions updateOptions) {

		ClientUpdateManyOptions updateManyOptions = ClientUpdateManyOptions.clientUpdateManyOptions();
		updateManyOptions.arrayFilters(updateOptions.getArrayFilters());
		updateManyOptions.collation(updateOptions.getCollation());
		updateManyOptions.upsert(updateOptions.isUpsert());
		updateManyOptions.hint(updateOptions.getHint());
		updateManyOptions.hintString(updateOptions.getHintString());

		if (update instanceof List<?> pipeline) {
			return ClientNamespacedWriteModel.updateMany(namespace, query, (List<Document>) pipeline, updateManyOptions);
		} else if (update instanceof Document updateDocument) {
			return ClientNamespacedWriteModel.updateMany(namespace, query, updateDocument, updateManyOptions);
		} else {
			throw new IllegalArgumentException(
					"Update needs to be either a List or a Document, but was [%s]".formatted(ClassUtils.getUserClass(update)));
		}
	}

	static ClientNamespacedWriteModel updateOne(MongoNamespace namespace, Document query, Object update,
			UpdateOptions updateOptions) {

		ClientUpdateOneOptions updateOneOptions = ClientUpdateOneOptions.clientUpdateOneOptions();
		updateOneOptions.sort(updateOptions.getSort());
		updateOneOptions.arrayFilters(updateOptions.getArrayFilters());
		updateOneOptions.collation(updateOptions.getCollation());
		updateOneOptions.upsert(updateOptions.isUpsert());
		updateOneOptions.hint(updateOptions.getHint());
		updateOneOptions.hintString(updateOptions.getHintString());

		if (update instanceof List<?> pipeline) {
			return ClientNamespacedWriteModel.updateOne(namespace, query, (List<Document>) pipeline, updateOneOptions);
		} else if (update instanceof Document updateDocument) {
			return ClientNamespacedWriteModel.updateOne(namespace, query, updateDocument, updateOneOptions);
		} else {
			throw new IllegalArgumentException(
					"Update needs to be either a List or a Document, but was [%s]".formatted(ClassUtils.getUserClass(update)));
		}
	}

	static ClientNamespacedWriteModel removeMany(MongoNamespace namespace, Document query, DeleteOptions deleteOptions) {

		ClientDeleteManyOptions clientDeleteManyOptions = ClientDeleteManyOptions.clientDeleteManyOptions();
		clientDeleteManyOptions.collation(deleteOptions.getCollation());
		clientDeleteManyOptions.hint(deleteOptions.getHint());
		clientDeleteManyOptions.hintString(deleteOptions.getHintString());

		return ClientNamespacedWriteModel.deleteMany(namespace, query, clientDeleteManyOptions);
	}

	static ClientNamespacedWriteModel removeOne(MongoNamespace namespace, Document query, DeleteOptions deleteOptions) {

		ClientDeleteOneOptions clientDeleteOneOptions = ClientDeleteOneOptions.clientDeleteOneOptions();
		// TODO: open an issue with MongoDB to enable sort for deleteOne
		clientDeleteOneOptions.collation(deleteOptions.getCollation());
		clientDeleteOneOptions.hint(deleteOptions.getHint());
		clientDeleteOneOptions.hintString(deleteOptions.getHintString());

		return ClientNamespacedWriteModel.deleteOne(namespace, query, clientDeleteOneOptions);
	}

	static ClientNamespacedWriteModel replaceOne(MongoNamespace namespace, Document query, Document replacement,
			UpdateOptions updateOptions) {

		ClientReplaceOneOptions replaceOptions = ClientReplaceOneOptions.clientReplaceOneOptions();
		replaceOptions.sort(updateOptions.getSort());
		replaceOptions.upsert(updateOptions.isUpsert());
		replaceOptions.hint(updateOptions.getHint());
		replaceOptions.hintString(updateOptions.getHintString());
		replaceOptions.collation(updateOptions.getCollation());

		return ClientNamespacedWriteModel.replaceOne(namespace, query, replacement, replaceOptions);
	}
}
