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
package org.springframework.data.mongodb.core;

import java.util.ArrayList;
import java.util.List;

import org.bson.types.ObjectId;

public class PersonWithAList {

	private ObjectId id;

	private String firstName;

	private int age;

	private List<String> wishList = new ArrayList<String>();

	private List<Friend> friends = new ArrayList<Friend>();

	public ObjectId getId() {
		return id;
	}

	public void setId(ObjectId id) {
		this.id = id;
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	public List<String> getWishList() {
		return wishList;
	}

	public void setWishList(List<String> wishList) {
		this.wishList = wishList;
	}

	public void addToWishList(String wish) {
		this.wishList.add(wish);
	}

	public List<Friend> getFriends() {
		return friends;
	}

	public void setFriends(List<Friend> friends) {
		this.friends = friends;
	}

	public void addFriend(Friend friend) {
		this.friends.add(friend);
	}

	@Override
	public String toString() {
		return "PersonWithAList [id=" + id + ", firstName=" + firstName + ", age=" + age + ", wishList=" + wishList + "]";
	}

}
