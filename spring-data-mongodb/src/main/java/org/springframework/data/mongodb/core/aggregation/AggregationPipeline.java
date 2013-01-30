/*
 * Copyright 2011-2012 the original author or authors.
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
package org.springframework.data.mongodb.core.aggregation;

import java.util.ArrayList;
import java.util.List;

import org.springframework.util.Assert;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import com.mongodb.util.JSONParseException;

/**
 * Holds the operation of an aggregation pipeline.
 * 
 * @author Tobias Trelle
 */
public class AggregationPipeline {

	private List<DBObject> ops = new ArrayList<DBObject>();

	public AggregationPipeline(String... operations) {
		Assert.notNull(operations, "Aggregation pipeline operations are missing");

		if (operations.length > 0) {
			for (int i = 0; i < operations.length; i++) {
				ops.add( parseJson(operations[i]) );
			}
		}

	}

	public List<DBObject> getOps() {
		return ops;
	}

	private DBObject parseJson(String json) {
		try {
			return (DBObject) JSON.parse(json);
		} catch (JSONParseException e) {
			throw new IllegalArgumentException("Not a valid JSON document: " + json, e);
		}
	}

}
