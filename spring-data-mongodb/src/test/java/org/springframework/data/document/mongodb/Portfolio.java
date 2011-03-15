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
package org.springframework.data.document.mongodb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Portfolio {

  private String portfolioName;
  private User user;
  private List<Trade> trades;
  private Map<String, Integer> positions;
  private Map<String, Person> portfolioManagers;

  public Map<String, Person> getPortfolioManagers() {
    return portfolioManagers;
  }

  public void setPortfolioManagers(Map<String, Person> portfolioManagers) {
    this.portfolioManagers = portfolioManagers;
  }

  public Map<String, Integer> getPositions() {
    return positions;
  }

  public void setPositions(Map<String, Integer> positions) {
    this.positions = positions;
  }

  public Portfolio() {
    trades = new ArrayList<Trade>();
  }

  public String getPortfolioName() {
    return portfolioName;
  }

  public void setPortfolioName(String portfolioName) {
    this.portfolioName = portfolioName;
  }

  public List<Trade> getTrades() {
    return trades;
  }

  public void setTrades(List<Trade> trades) {
    this.trades = trades;
  }

  public User getUser() {
    return user;
  }

  public void setUser(User user) {
    this.user = user;
  }
}
