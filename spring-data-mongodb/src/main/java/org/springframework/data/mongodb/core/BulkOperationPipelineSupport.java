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
import java.util.stream.Collectors;

import org.springframework.lang.CheckReturnValue;

/**
 * Shared support for bulk operation pipelines. A pipeline holds a sequence of
 * {@link BulkOperationPipelineItem pipeline items}. Each item is
 * <ul>
 * <li>{@link BulkOperationPipelineItem#map(Object) mapped} when appended (e.g. domain → BSON),</li>
 * <li>{@link BulkOperationPipelineItem#prepareForWrite(Object) prepared} when building write models for execution,</li>
 * <li>{@link BulkOperationPipelineItem#finish(Object) finished} after execution (e.g. after-save events).</li>
 * </ul>
 * This allows both collection-bound bulk operations ({@link DefaultBulkOperations}) and namespaced bulk operations
 * ({@link NamespacedBulkOperationSupport}) to share the same pipeline lifecycle.
 *
 * @param <C> context type used for mapping and lifecycle (e.g. {@link DefaultBulkOperations.BulkOperationContext} or
 *          {@link NamespacedBulkOperationSupport.NamespacedBulkOperationContext}).
 * @param <M> the write model type produced by items (e.g. {@link com.mongodb.client.model.WriteModel WriteModel}&lt;Document&gt;
 *          or {@link com.mongodb.client.model.bulk.ClientNamespacedWriteModel}).
 * @author Christoph Strobl
 */
final class BulkOperationPipelineSupport<C, M> {

	private final List<BulkOperationPipelineItem<C, M>> pipeline = new ArrayList<>();
	private final C context;

	BulkOperationPipelineSupport(C context) {
		this.context = context;
	}

	/**
	 * Append an item to the pipeline. The item is {@link BulkOperationPipelineItem#map(Object) mapped} immediately
	 * with the pipeline context.
	 *
	 * @param item the pipeline item to append (must not be {@literal null}).
	 */
	void append(BulkOperationPipelineItem<C, M> item) {
		pipeline.add(item.map(context));
	}

	/**
	 * Build the list of write models by calling {@link BulkOperationPipelineItem#prepareForWrite(Object)} on each
	 * pipeline item.
	 *
	 * @return the list of write models to pass to the driver.
	 */
	List<M> models() {
		return pipeline.stream().map(it -> it.prepareForWrite(context)).collect(Collectors.toList());
	}

	/**
	 * Run post-processing (e.g. after-save events and callbacks) by calling
	 * {@link BulkOperationPipelineItem#finish(Object)} on each pipeline item.
	 */
	void postProcess() {
		pipeline.forEach(it -> it.finish(context));
	}

	/**
	 * A single item in a bulk operation pipeline. Implementations are responsible for mapping domain data to the
	 * driver representation, building the write model, and running lifecycle hooks.
	 *
	 * @param <C> context type.
	 * @param <M> write model type.
	 */
	interface BulkOperationPipelineItem<C, M> {

		/**
		 * Map this item using the given context (e.g. map domain objects to BSON). Called when the item is appended
		 * to the pipeline. May return {@code this} if already mapped or a new instance with mapped state.
		 *
		 * @param context the bulk operation context (must not be {@literal null}).
		 * @return this item or a mapped copy (never {@literal null}).
		 */
		@CheckReturnValue
		BulkOperationPipelineItem<C, M> map(C context);

		/**
		 * Build the write model for the driver. Lifecycle (e.g. before-save events) may be run here. Called when
		 * collecting models for execution.
		 *
		 * @param context the bulk operation context (must not be {@literal null}).
		 * @return the write model (never {@literal null}).
		 */
		M prepareForWrite(C context);

		/**
		 * Run post-write lifecycle (e.g. after-save events and callbacks). Called after execution. Default is no-op.
		 *
		 * @param context the bulk operation context (must not be {@literal null}).
		 */
		default void finish(C context) {}
	}
}
