package com.olson1998.cassandra.adapter;

import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;

public class StatementConfigInit<A, D> {

    private final A argument;

    private final Session session;

    private final Mapper<D> defaultMapper;

    public StatementConfig<A, D> applyQuery(String query){
        return new StatementConfig<>(query, argument, session, defaultMapper);
    }

    protected StatementConfigInit(A argument, Session session, Mapper<D> defaultMapper) {
        this.argument = argument;
        this.session = session;
        this.defaultMapper = defaultMapper;
    }
}
