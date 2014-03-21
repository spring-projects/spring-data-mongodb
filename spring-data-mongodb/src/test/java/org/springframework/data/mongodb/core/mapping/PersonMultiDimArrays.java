/*
 * Copyright 2011-2013 the original author or authors.
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
package org.springframework.data.mongodb.core.mapping;

import org.springframework.data.annotation.Id;

/**
 * @author Jon Brisbin
 */
@Document
public class PersonMultiDimArrays extends BasePerson {

	@Id private String id;
	private String[][] grid;

	public PersonMultiDimArrays(Integer ssn, String firstName, String lastName, String[][] grid) {
		super(ssn, firstName, lastName);
		this.grid = grid;
	}

	public String[][] getGrid() {
		return grid;
	}

	public void setGrid(String[][] grid) {
		this.grid = grid;
	}
}
