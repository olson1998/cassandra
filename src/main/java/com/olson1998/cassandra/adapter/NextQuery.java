package com.olson1998.cassandra.adapter;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.google.common.reflect.TypeToken;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class NextQuery<A, D> {

    private final A argument;

    private final Session session;

    private final Mapper<D> defaultMapper;

    public NextQuery<List<D>, D> thenRun(String query){
        var resultSet = session.execute(query);
        var result = defaultMapper.map(resultSet);
        var args = result.all();
        return new DefaultNextQuery<>(args, session, defaultMapper);
    }

    public NextQuery<List<D>, D> thenRun(Function<A, String> queryProducer){
        var query = queryProducer.apply(argument);
        var resultSet = session.execute(query);
        var result = defaultMapper.map(resultSet);
        var args = result.all();
        return new DefaultNextQuery<>(args, session, defaultMapper);
    }

    public <P> NextQuery<List<P>, D> thenRun(String query, Mapper<P> producedMapper){
        var resultSet = session.execute(query);
        var result = producedMapper.map(resultSet);
        var args = result.all();
        return new DefaultNextQuery<>(args, session, defaultMapper);
    }

    public  <P> NextQuery<List<P>, D> thenRun(String query, TypeToken<P> producedType){
        var resultSet = session.execute(query);
        var args = ResultSetDispatcher.dispatchList(resultSet, producedType);
        return new DefaultNextQuery<>(args, session, defaultMapper);
    }

    public  <P> NextQuery<List<P>, D> thenRun(String query, Class<P> producedClass){
        var token = TypeToken.of(producedClass);
        return thenRun(query, token);
    }

    public <P> NextQuery<List<P>, D> thenRun(Function<A, String> queryProducer, TypeToken<P> producedType){
        var query = queryProducer.apply(argument);
        var resultSet = session.execute(query);
        var args = ResultSetDispatcher.dispatchList(resultSet, producedType);
        return new DefaultNextQuery<>(args, session, defaultMapper);
    }

    public <P> NextQuery<List<P>, D> thenRun(Function<A, String> queryProducer, Class<P> producedType){
        var token = TypeToken.of(producedType);
        return thenRun(queryProducer, token);
    }

    public NextQuery<D, D> thenRunForUnique(String query){
        var resultSet = session.execute(query);
        var result = defaultMapper.map(resultSet);
        var arg = result.one();
        return new DefaultNextQuery<>(arg, session, defaultMapper);
    }

    public NextQuery<D, D> thenRunForUnique(Function<A, String> queryProducer){
        var query = queryProducer.apply(argument);
        var resultSet = session.execute(query);
        var result = defaultMapper.map(resultSet);
        var arg = result.one();
        return new DefaultNextQuery<>(arg, session, defaultMapper);
    }

    public <P> NextQuery<P, D> thenRunForUnique(String query, Mapper<P> producedMapper){
        var resultSet = session.execute(query);
        var result = producedMapper.map(resultSet);
        var arg = result.one();
        return new DefaultNextQuery<>(arg, session, defaultMapper);
    }

    public <P> NextQuery<P, D> thenRunForUnique(String query, TypeToken<P> producedType){
        var resultSet = session.execute(query);
        var column = getUniqueColumnName(resultSet);
        var arg = resultSet.one().get(column, producedType);
        return new DefaultNextQuery<>(arg, session, defaultMapper);
    }

    public <P> NextQuery<P, D> thenRunForUnique(String query, Class<P> producedClass){
        var token = TypeToken.of(producedClass);
        return thenRunForUnique(query, token);
    }

    public <P> NextQuery<P, D> thenRunForUnique(Function<A, String> queryProducer, TypeToken<P> producedType){
        var query = queryProducer.apply(argument);
        var resultSet = session.execute(query);
        var column = getUniqueColumnName(resultSet);
        var arg = resultSet.one().get(column, producedType);
        return new DefaultNextQuery<>(arg, session, defaultMapper);
    }

    public StatementConfigInit<A, D> thenComposeStatement(){
        return new StatementConfigInit<>(argument, session, defaultMapper);
    }

    public NextQuery<Void, D> thenExecute(String query){
        session.execute(query);
        return new DefaultNextQuery<>(null, session, defaultMapper);
    }

    public <P> NextQuery<P, D> thenProduce(Supplier<P> argumentProducer){
        var arg = argumentProducer.get();
        return new DefaultNextQuery<>(arg, session, defaultMapper);
    }

    public NextQuery<A, D> thenSnitch(Consumer<A> snitchConsumer){
        snitchConsumer.accept(argument);
        return this;
    }

    public NextQuery<Void, D> thenAccept(Consumer<A> argumentConsumer){
        argumentConsumer.accept(argument);
        return new DefaultNextQuery<>(null, session, defaultMapper);
    }

    public <P> NextQuery<P, D> thenMap(Function<A, P> mappingFun){
        var arg = mappingFun.apply(argument);
        return new DefaultNextQuery<>(arg, session, defaultMapper);
    }

    public A commit(){
        session.close();
        return argument;
    }

    public void close(){
        session.close();
    }

    protected String getUniqueColumnName(ResultSet resultSet) {
        var columns = resultSet.getColumnDefinitions().asList();
        if(columns.size() ==1){
            return columns.get(0).getName();
        }else {
            return "";
        }
    }

    protected NextQuery(A argument, Session session, Mapper<D> defaultMapper) {
        this.argument = argument;
        this.session = session;
        this.defaultMapper = defaultMapper;
    }


}
