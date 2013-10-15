package org.springframework.data.mongodb.repository;

import java.math.BigInteger;

import org.springframework.data.mongodb.core.mapping.Document;

@Document
public class Boss {

	BigInteger id;
	String name;
	
}
