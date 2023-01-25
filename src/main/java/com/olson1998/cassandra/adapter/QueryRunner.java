package com.olson1998.cassandra.adapter;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingConfiguration;
import com.datastax.driver.mapping.MappingManager;

import java.util.List;
import java.util.Set;

public abstract class QueryRunner<T> {

    private final Class<T> defaultMappingClass;

    private final MappingConfiguration mappingConfiguration;

    abstract String keyspace();

    abstract Cluster cluster();

    private List<T> run(String query){
        try(var session = cluster().connect()){
            var mapper = getMapper(session);
            var resultSet = session.execute(query);
            return mapper.map(resultSet).all();
        }
    }

    private List<T> run(Statement statement){
        try(var session = cluster().connect()){
            var mapper = getMapper(session);
            var resultSet = session.execute(statement);
            return mapper.map(resultSet).all();
        }
    }

    public T runForUnique(String query){
        try(var session = cluster().connect()){
            var mapper = getMapper(session);
            var resultSet = session.execute(query);
            return mapper.map(resultSet).one();
        }
    }

    public T runForUnique(Statement statement){
        try(var session = cluster().connect()){
            var mapper = getMapper(session);
            var resultSet = session.execute(statement);
            return mapper.map(resultSet).one();
        }
    }

    public T save(T entity){
        try(var session = cluster().connect()){
            var mapper = getMapper(session);
            mapper.save(entity);
            return entity;
        }
    }

    public Set<T> save(Set<T> entities){
        try(var session = cluster().connect()){
            var mapper = getMapper(session);
            entities.forEach(mapper::save);
            return entities;
        }
    }

    public void delete(T entity){
        try(var session = cluster().connect()){
            var mapper = getMapper(session);
            mapper.delete(entity);
        }
    }

    public void delete(Set<T> entities){
        try(var session = cluster().connect()){
            var mapper = getMapper(session);
            entities.forEach(mapper::delete);
        }
    }

    public QuerySequence<T> runSequence(){
        var session = cluster().connect();
        var mapper = getMapper(session);
        return new QuerySequence<>(session, mapper);
    }


    private Mapper<T> getMapper(Session session){
        var keyspace = keyspace();
        return new MappingManager(session, mappingConfiguration)
                .mapper(defaultMappingClass, keyspace);
    }

    public QueryRunner(Class<T> defaultMappingClass, MappingConfiguration mappingConfiguration) {
        this.defaultMappingClass = defaultMappingClass;
        this.mappingConfiguration = mappingConfiguration;
    }
}
