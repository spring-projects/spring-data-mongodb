/*
 * Copyright 2026. the original author or authors.
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

/*
 * Copyright 2026 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.mongodb.core.query.Criteria.where;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.BulkOperations.BulkMode;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.MongoTestTemplate;
import org.springframework.data.mongodb.test.util.Template;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.bulk.ClientBulkWriteResult;

/**
 * @author Christoph Strobl
 * @since 2026/01
 */
public class NamespaceBulkOperationIntegrationTests {

	static final String COLLECTION_NAME = "bulk_ops";

	@Client static MongoClient mongoClient;

	@Template(initialEntitySet = BaseDoc.class) //
	static MongoTestTemplate operations;

	@BeforeEach
	public void setUp() {
		operations.flushDatabase();
	}

	@Test // GH-5087
	void bulkWriteMultipleCollections() {

		/*
		NamespaceBulkOperations and NamespaceAwareBulkOperations are new apis that allow to do bulk operations for MongoDB via an api that allows to be used with multiple mongodb collections.
		The existing BulkOperations API is tied to a single collection.
		You need to create tests for the new API similar to the existing ones in DefaultBulkOperationsIntegrationTests.
		The tests need to make sure operations are executed in different collections.
		you can see an example of this and how to use the new API in  NamespaceBulkOperationIntegrationTests


		 */

		operations.flushDatabase();

		BaseDoc doc1 = new BaseDoc();
		doc1.id = "id-doc1";
		doc1.value = "value-doc1";

		BaseDoc doc2 = new BaseDoc();
		doc2.id = "id-doc2";
		doc2.value = "value-doc2";

		NamespaceBulkOperations bulkOps = operations.bulkOps(BulkMode.ORDERED);
		ClientBulkWriteResult result = bulkOps
				.inCollection(BaseDoc.class,
						ops -> ops.insert(doc1).insert(doc2).upsert(where("_id").is("id-doc3"),
								new Update().set("value", "upserted")))
				.inCollection(SpecialDoc.class).insert(new SpecialDoc()).execute();

		assertThat(result.getUpsertedCount()).isOne();
		assertThat(result.getInsertedCount()).isEqualTo(3);

		Long inBaseDocCollection = operations.execute(BaseDoc.class, MongoCollection::countDocuments);
		Long inSpecialCollection = operations.execute(SpecialDoc.class, MongoCollection::countDocuments);
		assertThat(inBaseDocCollection).isEqualTo(3L);
		assertThat(inSpecialCollection).isOne();
	}

	// @Test // GH-5087
	// void exploreItOnClient() {
	//
	// ClientBulkWriteOperation op = null;
	// // op.execute()
	//
	// mongoClient.getDatabase("test").getCollection("pizzas").drop();
	// mongoClient.getDatabase("test").getCollection("pizzaOrders").drop();
	//
	// // command:
	// MongoDatabase db = operations.getDb();
	// MongoTemplate template = operations;
	//
	// Document commandDocument = new Document("bulkWrite", new BsonInt32(1)).append("errorsOnly", BsonBoolean.TRUE)
	// .append("ordered", BsonBoolean.TRUE);
	// List<Document> bulkOperations = new ArrayList<>();
	// bulkOperations
	// .add(Document.parse("{ insert: 0, document: { _id: 5, type: 'sausage', size: 'small', price: 12 } }"));
	// bulkOperations.add(Document.parse("{ insert: 1, document: { _id: 4, type: 'vegan cheese', number: 16 } }"));
	// commandDocument.put("ops", bulkOperations);
	//
	// List<Document> namespaceInfo = new ArrayList<>();
	// namespaceInfo.add(Document.parse("{ns: '%s.pizzas'}".formatted(template.getDb().getName())));
	// namespaceInfo.add(Document.parse("{ns: '%s.pizzaOrders'}".formatted(template.getDb().getName())));
	// commandDocument.put("nsInfo", namespaceInfo);
	//
	// template.getMongoDatabaseFactory().getMongoDatabase("admin").runCommand(commandDocument);
	// }
	//
	// @Test
	// void hackSomePotentialApi() {
	//
	// MongoNamespace pizzasNamespace = new MongoNamespace(operations.getDb().getName(), "pizzas");
	//
	// ConcreteClientNamespacedInsertOneModel insert1 = new ConcreteClientNamespacedInsertOneModel(pizzasNamespace,
	// new ConcreteClientInsertOneModel(
	// Document.parse("{ insert: 0, document: { _id: 5, type: 'sausage', size: 'small', price: 12 } }")));
	// insert1.getNamespace();
	// insert1.getModel();
	// }
}
