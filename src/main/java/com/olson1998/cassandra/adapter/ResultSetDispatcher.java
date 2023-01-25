package com.olson1998.cassandra.adapter;

import com.datastax.driver.core.ResultSet;
import com.google.common.reflect.TypeToken;

import java.util.ArrayList;
import java.util.List;

public class ResultSetDispatcher {

    public static <P> P dispatch(ResultSet resultSet, TypeToken<P> producedType){
        var column = getUniqueColumnName(resultSet);
        return resultSet.one().get(column, producedType);
    }

    public static <P> List<P> dispatchList(ResultSet resultSet, TypeToken<P> producedType){
        var args = new ArrayList<P>();
        var column = getUniqueColumnName(resultSet);
        resultSet.forEach(row -> {
            row.get(column, producedType);
        });
        return args;
    }

    protected static String getUniqueColumnName(ResultSet resultSet){
        var columns = resultSet.getColumnDefinitions().asList();
        if(columns.size() ==1){
            return columns.get(0).getName();
        }else {
            return "";
        }
    }

    private ResultSetDispatcher() {
    }
}
