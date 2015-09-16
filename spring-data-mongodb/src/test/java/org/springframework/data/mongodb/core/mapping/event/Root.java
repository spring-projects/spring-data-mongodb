/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.springframework.data.mongodb.core.mapping.event;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 *
 * @author Jordi Llach
 */
@Document
public class Root 
extends      Parent
implements   Serializable {
    
    private static final long serialVersionUID = -3211692873265644541L;
    
    @Id
    private Long id;
    
    // simple
    private Related embed;
    
    // dbref simple
    @DBRef
    private Related ref;
    @DBRef(lazy = true)
    private Related lazyRef;
    
    // collection support
    @DBRef
    private List<Related> listRef;
    @DBRef(lazy = true)
    private List<Related> listLazy;
    
    // map support
    @DBRef
    private Map<String, Related> mapRef;
    @DBRef(lazy = true)
    private Map<String, Related> mapLazy;
    
    @PersistenceConstructor
    public Root(Long id, Related embed, Related ref, Related lazyRef, List<Related> listRef, List<Related> listLazy,
            Map<String, Related> mapRef, Map<String, Related> mapLazy) {
        this.id = id;
        this.embed = embed;
        this.ref   = ref;
        this.lazyRef = lazyRef;
        this.listRef = listRef;
        this.listLazy = listLazy;
        this.mapRef = mapRef;
        this.mapLazy = mapLazy;
    }
    
    public Long getId() {
        return id;
    }

    public Related getEmbed() {
        return embed;
    }

    public Related getRef() {
        return ref;
    }

    public Related getLazyRef() {
        return lazyRef;
    }

    public List<Related> getListRef() {
        return listRef;
    }

    public List<Related> getListLazy() {
        return listLazy;
    }

    public Map<String, Related> getMapRef() {
        return mapRef;
    }

    public Map<String, Related> getMapLazy() {
        return mapLazy;
    }
}