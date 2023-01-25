package com.olson1998.cassandra.adapter;

import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;

public class DefaultNextQuery<A, D> extends NextQuery<A, D>{
    protected DefaultNextQuery(A argument, Session session, Mapper<D> defaultMapper) {
        super(argument, session, defaultMapper);
    }
}
