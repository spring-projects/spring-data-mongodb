package org.springframework.data.mongodb.repository;

import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;

@Document
public class Book {
    public String title;
    public Integer price;
    public Boolean available;
    public List<String> categories;
}
