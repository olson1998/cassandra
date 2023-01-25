package com.olson1998.cassandra.adapter;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.mapping.Mapper;

import java.util.HashMap;
import java.util.Map;

public class StatementConfig<A, D> {

    private final String queryFacade;

    private final Map<String, Object> args;

    private final A argument;

    private final Session session;

    private final Mapper<D> defaultMapper;

    public StatementConfig<A, D> putArg(String name){
        args.put(name, argument);
        return this;
    }

    public StatementConfig<A, D> putArg(String name, Object arg){
        args.put(name, arg);
        return this;
    }

    public StatementFinalizer<D> finish(){
        var statement =  new SimpleStatement(queryFacade, args);
        return new StatementFinalizer<>(statement, session, defaultMapper);
    }

    protected StatementConfig(String queryFacade, A argument, Session session, Mapper<D> defaultMapper) {
        this.queryFacade = queryFacade;
        this.args = new HashMap<>();
        this.argument = argument;
        this.session = session;
        this.defaultMapper = defaultMapper;
    }
}
