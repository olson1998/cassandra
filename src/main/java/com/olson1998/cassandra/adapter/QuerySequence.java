package com.olson1998.cassandra.adapter;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.google.common.reflect.TypeToken;

import java.util.ArrayList;
import java.util.List;

public class QuerySequence<D> {

    private final Session session;

    private final Mapper<D> defaultMapper;

    public NextQuery<List<D>, D> run(String query){
        var resultSet = session.execute(query);
        var args = defaultMapper.map(resultSet).all();
        return new DefaultNextQuery<>(args, session, defaultMapper);
    }

    public  <P> NextQuery<List<P>, D> run(String query, TypeToken<P> producedType){
        var resultSet = session.execute(query);
        var args = ResultSetDispatcher.dispatchList(resultSet, producedType);
        return new DefaultNextQuery<>(args, session, defaultMapper);
    }

    public  <P> NextQuery<List<P>, D> run(String query, Class<P> producedType){
        var token = TypeToken.of(producedType);
        return run(query, token);
    }

    public <P> NextQuery<P, D> runForUnique(String query, TypeToken<P> producedType){
        var resultSet = session.execute(query);
        var arg = ResultSetDispatcher.dispatch(resultSet, producedType);
        return new DefaultNextQuery<>(arg, session, defaultMapper);
    }

    public <P> NextQuery<P, D> runForUnique(String query, Class<P> producedType){
        var token = TypeToken.of(producedType);
        return runForUnique(query, token);
    }

    public NextQuery<D, D> runForUnique(String query){
        var resultSet = session.execute(query);
        var arg = defaultMapper.map(resultSet).one();
        return new DefaultNextQuery<>(arg, session, defaultMapper);
    }

    public NextQuery<Void, D> execute(String query){
        session.execute(query);
        return new DefaultNextQuery<>(null, session, defaultMapper);
    }

    public NextQuery<Void, D> start(){
        return new DefaultNextQuery<>(null, session, defaultMapper);
    }

    protected QuerySequence(Session session, Mapper<D> defaultMapper) {
        this.session = session;
        this.defaultMapper = defaultMapper;
    }
}
