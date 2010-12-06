/*
 * Copyright 2010 the original author or authors.
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
package org.springframework.data.document.mongodb;

import org.junit.Assert;
import org.junit.Test;

public class UpdateBuilderTests {

	@Test
	public void testSetUpdate() {
		UpdateBuilder ub = new UpdateBuilder()
			.set("directory", "/Users/Test/Desktop");
		Assert.assertEquals("{ \"$set\" : { \"directory\" : \"/Users/Test/Desktop\"}}", ub.build().toString());
	}

	@Test
	public void testIncUpdate() {
		UpdateBuilder ub = new UpdateBuilder()
			.inc("size", 1);
		Assert.assertEquals("{ \"$inc\" : { \"size\" : 1}}", ub.build().toString());
	}

	@Test
	public void testIncAndSetUpdate() {
		UpdateBuilder ub = new UpdateBuilder()
			.inc("size", 1)
			.set("directory", "/Users/Test/Desktop");
		Assert.assertEquals("{ \"$inc\" : { \"size\" : 1} , \"$set\" : { \"directory\" : \"/Users/Test/Desktop\"}}", 
				ub.build().toString());
	}

	@Test
	public void testUnset() {
		UpdateBuilder ub = new UpdateBuilder()
			.unset("directory");
		Assert.assertEquals("{ \"$unset\" : { \"directory\" : 1}}", ub.build().toString());
	}

}
