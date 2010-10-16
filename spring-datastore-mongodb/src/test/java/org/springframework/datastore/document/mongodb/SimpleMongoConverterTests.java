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
package org.springframework.datastore.document.mongodb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;


public class SimpleMongoConverterTests {

	@Test
	public void notNestedObject() {
		User user = new User();
		user.setAccountName("My Account");
		user.setUserName("Mark");
		SimpleMongoConverter converter = createConverter();
		DBObject dbo = new BasicDBObject();
		converter.write(user, dbo);
		Assert.assertEquals("My Account", dbo.get("accountName"));
		Assert.assertEquals("Mark", dbo.get("userName"));
		
		User u = (User) converter.read(User.class, dbo);
		
		Assert.assertEquals("My Account", u.getAccountName());
		Assert.assertEquals("Mark", u.getUserName());
	}
	
	@Test
	public void nestedObject() {
		Portfolio p = createPortfolioWithNoTrades();
		SimpleMongoConverter converter = createConverter();
		DBObject dbo = new BasicDBObject();
		converter.write(p, dbo);
		Assert.assertEquals("High Risk Trading Account", dbo.get("portfolioName"));
		Assert.assertTrue(dbo.containsField("user"));
		
		Portfolio cp = (Portfolio) converter.read(Portfolio.class, dbo);		
		Assert.assertEquals("High Risk Trading Account", cp.getPortfolioName());
		Assert.assertEquals("Joe Trader", cp.getUser().getUserName());
		Assert.assertEquals("ACCT-123", cp.getUser().getAccountName());
		
	}

	private SimpleMongoConverter createConverter() {
		SimpleMongoConverter converter = new SimpleMongoConverter();
		ConversionContext context = new ConversionContext();
		context.setBuilderLookup(new BuilderLookup());
		converter.setConversionContext(context);
		return converter;
	}
	
	@Test
	public void instanceOfTests() {
		List<String> list = new ArrayList<String>();
		Assert.assertTrue(list instanceof Collection);
		
		List objList = new ArrayList();
		Assert.assertTrue(objList instanceof Collection);
		
		
	}
	
    protected Portfolio createPortfolioWithNoTrades()
    {
        Portfolio portfolio = new Portfolio();
        User user = new User();
        user.setUserName("Joe Trader");
        user.setAccountName("ACCT-123");
        portfolio.setUser(user);
        portfolio.setPortfolioName("High Risk Trading Account");
        return portfolio;
    }
}
