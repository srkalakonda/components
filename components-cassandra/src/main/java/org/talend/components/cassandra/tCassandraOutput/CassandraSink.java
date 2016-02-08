package org.talend.components.cassandra.tCassandraOutput;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.talend.components.api.exception.TalendConnectionException;
import org.talend.components.api.runtime.output.Sink;
import org.talend.components.api.runtime.output.Writer;
import org.talend.components.api.properties.ComponentProperties;
import org.talend.components.api.runtime.row.BaseRowStruct;
import org.talend.components.cassandra.type.CassandraBaseType;
import org.talend.daikon.schema.Schema;
import org.talend.daikon.schema.SchemaElement;
import org.talend.daikon.schema.internal.DataSchemaElement;
import org.talend.daikon.schema.type.TypeMapping;

import java.util.List;

/**
 * Created by bchen on 16-1-17.
 */
public class CassandraSink implements Sink {
    Session connection;
    Cluster cluster;
    BoundStatement boundStatement;
    tCassandraOutputDIProperties props;

    @Override
    public void init(ComponentProperties properties) throws TalendConnectionException {
        props = (tCassandraOutputDIProperties) properties;
        Cluster.Builder clusterBuilder = Cluster.builder().addContactPoints(props.host.getStringValue().split(",")).withPort(Integer.valueOf(props.port.getStringValue()));
        if (props.useAuth.getBooleanValue()) {
            clusterBuilder.withCredentials(props.username.getStringValue(), props.password.getStringValue());
        }
        cluster = clusterBuilder.build();
        connection = cluster.connect();

        CQLManager cqlManager = new CQLManager(props, ((Schema) props.schema.schema.getValue()).getRoot().getChildren());
        PreparedStatement preparedStatement = connection.prepare(cqlManager.generatePreActionSQL());
        if (props.useUnloggedBatch.getBooleanValue()) {
            //TODO
        } else {
            boundStatement = new BoundStatement(preparedStatement);
        }

    }

    @Override
    public void close() {

    }

    @Override
    public CassandraRecordWriter getRecordWriter() {
        return new CassandraRecordWriter(boundStatement);
    }

    @Override
    public void initDest() {
        //create keyspace/columnFamily
    }

    @Override
    public List<SchemaElement> getSchema() {
        return null;
    }

    public class CassandraRecordWriter implements Writer {
        BoundStatement boundStatement;

        public CassandraRecordWriter(BoundStatement boundStatement) {
            this.boundStatement = boundStatement;
        }

        @Override
        public void write(BaseRowStruct rowStruct) {
            for (SchemaElement column : ((Schema) props.schema.schema.getValue()).getRoot().getChildren()) {
                DataSchemaElement col = (DataSchemaElement) column;
                try {
                    col.getAppColType().newInstance().assign2AValue(TypeMapping.convert(CassandraBaseType.FAMILY_NAME, col, rowStruct.get(col.getName())), boundStatement, col.getAppColName());
                } catch (InstantiationException e) {
                    e.printStackTrace();
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
            connection.execute(boundStatement);
        }

        @Override
        public void close() {

        }
    }
}
