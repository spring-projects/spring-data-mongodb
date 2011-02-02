/*
 * Copyright 2010-2011 the original author or authors.
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
package org.springframework.data.document.mongodb.builder;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.data.document.mongodb.builder.SortSpec.SortOrder;
import org.springframework.data.document.mongodb.builder.UpdateSpec;

public class UpdateTests {

	@Test
	public void testSet() {
		UpdateSpec u = new UpdateSpec()
			.set("directory", "/Users/Test/Desktop");
		Assert.assertEquals("{ \"$set\" : { \"directory\" : \"/Users/Test/Desktop\"}}", u.build().getUpdateObject().toString());
	}

	@Test
	public void testInc() {
		UpdateSpec u = new UpdateSpec()
			.inc("size", 1);
		Assert.assertEquals("{ \"$inc\" : { \"size\" : 1}}", u.build().getUpdateObject().toString());
	}

	@Test
	public void testIncAndSet() {
		UpdateSpec u = new UpdateSpec()
			.inc("size", 1)
			.set("directory", "/Users/Test/Desktop");
		Assert.assertEquals("{ \"$inc\" : { \"size\" : 1} , \"$set\" : { \"directory\" : \"/Users/Test/Desktop\"}}", 
				u.build().getUpdateObject().toString());
	}

	@Test
	public void testUnset() {
		UpdateSpec u = new UpdateSpec()
			.unset("directory");
		Assert.assertEquals("{ \"$unset\" : { \"directory\" : 1}}", u.build().getUpdateObject().toString());
	}

	@Test
	public void testPush() {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("name", "Sven");
		UpdateSpec u = new UpdateSpec()
			.push("authors", m);
		Assert.assertEquals("{ \"$push\" : { \"authors\" : { \"name\" : \"Sven\"}}}", u.build().getUpdateObject().toString());
	}

	@Test
	public void testPushAll() {
		Map<String, Object> m1 = new HashMap<String, Object>();
		m1.put("name", "Sven");
		Map<String, Object> m2 = new HashMap<String, Object>();
		m2.put("name", "Maria");
		UpdateSpec u = new UpdateSpec()
			.pushAll("authors", new Object[] {m1, m2});
		Assert.assertEquals("{ \"$pushAll\" : { \"authors\" : [ { \"name\" : \"Sven\"} , { \"name\" : \"Maria\"}]}}", u.build().getUpdateObject().toString());
	}

	@Test
	public void testAddToSet() {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("name", "Sven");
		UpdateSpec u = new UpdateSpec()
			.addToSet("authors", m);
		Assert.assertEquals("{ \"$addToSet\" : { \"authors\" : { \"name\" : \"Sven\"}}}", u.build().getUpdateObject().toString());
	}

	@Test
	public void testPop() {
		UpdateSpec u = new UpdateSpec()
			.pop("authors", UpdateSpec.Position.FIRST);
		Assert.assertEquals("{ \"$pop\" : { \"authors\" : -1}}", u.build().getUpdateObject().toString());
		u = new UpdateSpec()
			.pop("authors", UpdateSpec.Position.LAST);
		Assert.assertEquals("{ \"$pop\" : { \"authors\" : 1}}", u.build().getUpdateObject().toString());
	}

	@Test
	public void testPull() {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("name", "Sven");
		UpdateSpec u = new UpdateSpec()
			.pull("authors", m);
		Assert.assertEquals("{ \"$pull\" : { \"authors\" : { \"name\" : \"Sven\"}}}", u.build().getUpdateObject().toString());
	}

	@Test
	public void testPullAll() {
		Map<String, Object> m1 = new HashMap<String, Object>();
		m1.put("name", "Sven");
		Map<String, Object> m2 = new HashMap<String, Object>();
		m2.put("name", "Maria");
		UpdateSpec u = new UpdateSpec()
			.pullAll("authors", new Object[] {m1, m2});
		Assert.assertEquals("{ \"$pullAll\" : { \"authors\" : [ { \"name\" : \"Sven\"} , { \"name\" : \"Maria\"}]}}", u.build().getUpdateObject().toString());
	}

	@Test
	public void testRename() {
		UpdateSpec u = new UpdateSpec()
			.rename("directory", "folder");
		Assert.assertEquals("{ \"$rename\" : { \"directory\" : \"folder\"}}", u.build().getUpdateObject().toString());
	}

	@Test
	public void testWithSortAscending() {
		UpdateSpec u = new UpdateSpec();
		u.set("name",52);
		u.sort().on("name", SortOrder.ASCENDING);
		Assert.assertEquals("{ \"$set\" : { \"name\" : 52}}", u.build().getUpdateObject().toString());
		Assert.assertEquals("{ \"name\" : 1}", u.build().getSortObject().toString());
	}

	@Test
	public void testWithSortDescending() {
		UpdateSpec u = new UpdateSpec();
		u.set("name",52);
		u.sort().on("name", SortOrder.DESCENDING);
		Assert.assertEquals("{ \"$set\" : { \"name\" : 52}}", u.build().getUpdateObject().toString());
		Assert.assertEquals("{ \"name\" : -1}", u.build().getSortObject().toString());
	}
}
