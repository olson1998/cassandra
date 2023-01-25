package com.olson1998.cassandra.domain.model.port;

import com.datastax.driver.core.Cluster;

public interface ClusterManager<K> {

    Cluster getCluster(K key);

    String getKeyspace();
}
