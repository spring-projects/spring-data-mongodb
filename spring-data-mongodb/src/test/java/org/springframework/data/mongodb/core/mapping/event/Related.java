/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.springframework.data.mongodb.core.mapping.event;

import java.io.Serializable;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 *
 * @author Jordi Llach
 */
@Document
public class Related 
extends      Parent
implements   Serializable {

    private static final long serialVersionUID = -5719343113953216434L;
    
    @Id
    private Long   id;
    private String description;
    

    @PersistenceConstructor
    public Related(Long id, String description) {
        this.id = id;
        this.description = description;
    }
    
    public Long getId() {
        return id;
    }

    public String getDescription() {
        return description;
    }
}