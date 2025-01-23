package com.example.accessingdatacassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import org.crac.Context;
import org.crac.Core;
import org.crac.Resource;
import org.springframework.stereotype.Component;

@Component
public class CassandraConnectionManager implements Resource {
    private final CqlSession cqlSession;

    public CassandraConnectionManager(CqlSession cqlSession) {
        this.cqlSession = cqlSession;
        Core.getGlobalContext().register(this);
    }
    @Override
    public void afterRestore(Context<? extends Resource> context) throws Exception {
        try {
            if (cqlSession != null && !cqlSession.isClosed()) {
                System.out.println("TODO: Reinitialize session at restore");
            }
        } catch (Exception e) {
            System.out.println("Failed to reinitialize session at restore");
        }
    }

    @Override
    public void beforeCheckpoint(Context<? extends Resource> context) throws Exception {
        try {
            if (cqlSession != null && !cqlSession.isClosed()) {
                System.out.println("Closing session before checkpoint");
                cqlSession.close();
            }
        } catch (Exception e) {
            System.out.println("Failed to close session before checkpoint");
        }

    }
}
