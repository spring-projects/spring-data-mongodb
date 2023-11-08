/*
 * Copyright 2023 the original author or authors.
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
import java.util.Optional;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.context.ApplicationEvent;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mongodb.core.BulkOperations.BulkMode;
import org.springframework.data.mongodb.core.aggregation.AggregationOperationContext;
import org.springframework.data.mongodb.core.aggregation.AggregationUpdate;
import org.springframework.data.mongodb.core.aggregation.RelaxedTypeBasedAggregationOperationContext;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.convert.UpdateMapper;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.event.AfterSaveEvent;
import org.springframework.data.mongodb.core.mapping.event.BeforeSaveEvent;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.data.mongodb.core.query.UpdateDefinition.ArrayFilter;
import org.springframework.util.Assert;

import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.DeleteManyModel;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;

/**
 * Support class for bulk operations.
 *
 * @author Mark Paluch
 * @since 4.1
 */
abstract class BulkOperationsSupport {

	private final String collectionName;

	BulkOperationsSupport(String collectionName) {

		Assert.hasText(collectionName, "CollectionName must not be null nor empty");

		this.collectionName = collectionName;
	}

	/**
	 * Emit a {@link BeforeSaveEvent}.
	 *
	 * @param holder
	 */
	void maybeEmitBeforeSaveEvent(SourceAwareWriteModelHolder holder) {

		if (holder.model() instanceof InsertOneModel) {

			Document target = ((InsertOneModel<Document>) holder.model()).getDocument();
			maybeEmitEvent(new BeforeSaveEvent<>(holder.source(), target, collectionName));
		} else if (holder.model() instanceof ReplaceOneModel) {

			Document target = ((ReplaceOneModel<Document>) holder.model()).getReplacement();
			maybeEmitEvent(new BeforeSaveEvent<>(holder.source(), target, collectionName));
		}
	}

	/**
	 * Emit a {@link AfterSaveEvent}.
	 *
	 * @param holder
	 */
	void maybeEmitAfterSaveEvent(SourceAwareWriteModelHolder holder) {

		if (holder.model() instanceof InsertOneModel) {

			Document target = ((InsertOneModel<Document>) holder.model()).getDocument();
			maybeEmitEvent(new AfterSaveEvent<>(holder.source(), target, collectionName));
		} else if (holder.model() instanceof ReplaceOneModel) {

			Document target = ((ReplaceOneModel<Document>) holder.model()).getReplacement();
			maybeEmitEvent(new AfterSaveEvent<>(holder.source(), target, collectionName));
		}
	}

	WriteModel<Document> mapWriteModel(Object source, WriteModel<Document> writeModel) {

		if (writeModel instanceof UpdateOneModel<Document> model) {

			if (source instanceof AggregationUpdate aggregationUpdate) {

				List<Document> pipeline = mapUpdatePipeline(aggregationUpdate);
				return new UpdateOneModel<>(getMappedQuery(model.getFilter()), pipeline, model.getOptions());
			}

			return new UpdateOneModel<>(getMappedQuery(model.getFilter()), getMappedUpdate(model.getUpdate()),
					model.getOptions());
		}

		if (writeModel instanceof UpdateManyModel<Document> model) {

			if (source instanceof AggregationUpdate aggregationUpdate) {

				List<Document> pipeline = mapUpdatePipeline(aggregationUpdate);
				return new UpdateManyModel<>(getMappedQuery(model.getFilter()), pipeline, model.getOptions());
			}

			return new UpdateManyModel<>(getMappedQuery(model.getFilter()), getMappedUpdate(model.getUpdate()),
					model.getOptions());
		}

		if (writeModel instanceof DeleteOneModel<Document> model) {
			return new DeleteOneModel<>(getMappedQuery(model.getFilter()), model.getOptions());
		}

		if (writeModel instanceof DeleteManyModel<Document> model) {
			return new DeleteManyModel<>(getMappedQuery(model.getFilter()), model.getOptions());
		}

		return writeModel;
	}

	private List<Document> mapUpdatePipeline(AggregationUpdate source) {

		Class<?> type = entity().isPresent() ? entity().map(PersistentEntity::getType).get() : Object.class;
		AggregationOperationContext context = new RelaxedTypeBasedAggregationOperationContext(type,
				updateMapper().getMappingContext(), queryMapper());

		return new AggregationUtil(queryMapper(), queryMapper().getMappingContext()).createPipeline(source, context);
	}

	/**
	 * Emit a {@link ApplicationEvent} if event multicasting is enabled.
	 *
	 * @param event
	 */
	protected abstract void maybeEmitEvent(ApplicationEvent event);

	/**
	 * @return the {@link UpdateMapper} to use.
	 */
	protected abstract UpdateMapper updateMapper();

	/**
	 * @return the {@link QueryMapper} to use.
	 */
	protected abstract QueryMapper queryMapper();

	/**
	 * @return the associated {@link PersistentEntity}. Can be {@link Optional#empty()}.
	 */
	protected abstract Optional<? extends MongoPersistentEntity<?>> entity();

	protected Bson getMappedUpdate(Bson update) {
		return updateMapper().getMappedObject(update, entity());
	}

	protected Bson getMappedQuery(Bson query) {
		return queryMapper().getMappedObject(query, entity());
	}

	protected static BulkWriteOptions getBulkWriteOptions(BulkMode bulkMode) {

		BulkWriteOptions options = new BulkWriteOptions();

		return switch (bulkMode) {
			case ORDERED -> options.ordered(true);
			case UNORDERED -> options.ordered(false);
		};
	}

	/**
	 * @param filterQuery The {@link Query} to read a potential {@link Collation} from. Must not be {@literal null}.
	 * @param update The {@link Update} to apply
	 * @param upsert flag to indicate if document should be upserted.
	 * @return new instance of {@link UpdateOptions}.
	 */
	protected static UpdateOptions computeUpdateOptions(Query filterQuery, UpdateDefinition update, boolean upsert) {

		UpdateOptions options = new UpdateOptions();
		options.upsert(upsert);

		if (update.hasArrayFilters()) {
			List<Document> list = new ArrayList<>(update.getArrayFilters().size());
			for (ArrayFilter arrayFilter : update.getArrayFilters()) {
				list.add(arrayFilter.asDocument());
			}
			options.arrayFilters(list);
		}

		filterQuery.getCollation().map(Collation::toMongoCollation).ifPresent(options::collation);
		return options;
	}

	/**
	 * Value object chaining together an actual source with its {@link WriteModel} representation.
	 *
	 * @author Christoph Strobl
	 */
	record SourceAwareWriteModelHolder(Object source, WriteModel<Document> model) {
	}
}
