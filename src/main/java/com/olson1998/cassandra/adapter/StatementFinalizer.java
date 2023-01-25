package com.olson1998.cassandra.adapter;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.mapping.Mapper;
import com.google.common.reflect.TypeToken;

import java.util.List;

public class StatementFinalizer<D> {

    private final Statement statement;

    private final Session session;

    private final Mapper<D> defaultMapper;

    public NextQuery<List<D>, D> run(){
        var resultSet = session.execute(statement);
        var result = defaultMapper.map(resultSet);
        var args = result.all();
        return new DefaultNextQuery<>(args, session, defaultMapper);
    }

    public <P> NextQuery<P, D> runForUnique(TypeToken<P> producedType){
        var resultSet = session.execute(statement);
        var arg = ResultSetDispatcher.dispatch(resultSet, producedType);
        return new DefaultNextQuery<>(arg, session, defaultMapper);
    }

    public <P> NextQuery<P, D> runForUnique(Class<P> producedType){
        var token = TypeToken.of(producedType);
        return runForUnique(token);
    }

    public <P> NextQuery<List<P>, D> run(TypeToken<P> producedType){
        var resultSet = session.execute(statement);
        var args = ResultSetDispatcher.dispatchList(resultSet, producedType);
        return new DefaultNextQuery<>(args, session, defaultMapper);
    }

    public <P> NextQuery<List<P>, D> run(Class<P> producedType){
        var token = TypeToken.of(producedType);
        return run(token);
    }

    public NextQuery<D, D> runForUnique(){
        var resultSet = session.execute(statement);
        var result = defaultMapper.map(resultSet);
        var arg = result.one();
        return new DefaultNextQuery<>(arg, session, defaultMapper);
    }

    public NextQuery<Void, D> execute(){
        session.execute(statement);
        return new DefaultNextQuery<>(null, session, defaultMapper);
    }

    protected StatementFinalizer(Statement statement, Session session, Mapper<D> defaultMapper) {
        this.statement = statement;
        this.session = session;
        this.defaultMapper = defaultMapper;
    }
}
