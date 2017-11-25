package concurrencytest;

import org.apache.cayenne.Cayenne;
import org.apache.cayenne.DataObject;
import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.commitlog.CommitLogListener;
import org.apache.cayenne.commitlog.CommitLogModule;
import org.apache.cayenne.commitlog.model.ChangeMap;
import org.apache.cayenne.commitlog.model.ObjectChange;
import org.apache.cayenne.commitlog.model.ObjectChangeType;
import org.apache.cayenne.configuration.server.ServerRuntime;
import org.apache.cayenne.configuration.server.ServerRuntimeBuilder;
import org.apache.cayenne.query.SelectQuery;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import concurrencytest.data.Address;
import concurrencytest.data.Person;

public class Main {

    private static ServerRuntime _serverRuntime;

    private static ServerRuntime serverRuntime() {
        if( _serverRuntime == null ) {
            ServerRuntimeBuilder srtBuilder = ServerRuntime.builder()
                    .addConfig( "cayenne-concurrencytest.xml" )
                    .addModule( CommitLogModule.extend().addListener( AfterUpdateListener.class ).module() );

            HikariConfig config = new HikariConfig();
            config.setJdbcUrl( "jdbc:postgresql://localhost:5432/concurrency_test" );
            config.setUsername( "postgres" );
            config.setAutoCommit( false );

            HikariDataSource dataSource = new HikariDataSource( config );
            srtBuilder = srtBuilder.dataSource( dataSource );

            _serverRuntime = srtBuilder.build();
        }

        return _serverRuntime;
    }

    public static void main( String[] args ) {
        deleteAll();
        ObjectContext oc = newContext();
        Person person = oc.newObject( Person.class );
        person.setName( "Person" );
        oc.commitChanges();
        person.setName( "Hugi Þórðarson" );
        oc.commitChanges();
    }

    private static void deleteAll() {
        ObjectContext oc = serverRuntime().newContext();
        oc.deleteObjects( oc.select( new SelectQuery<>( Address.class ) ) );
        oc.deleteObjects( oc.select( new SelectQuery<>( Person.class ) ) );
        oc.commitChanges();
    }

    public static ObjectContext newContext() {
        return serverRuntime().newContext();
    }

    public static class AfterUpdateListener implements CommitLogListener {

        @Override
        public void onPostCommit( ObjectContext originatingContext, ChangeMap changes ) {

            for( ObjectChange objectChange : changes.getUniqueChanges() ) {
                if( objectChange.getType() == ObjectChangeType.UPDATE ) {
                    ObjectContext ctx = newContext();
                    DataObject person = (DataObject)Cayenne.objectForPK( ctx, objectChange.getPostCommitId() );

                    new Thread( () -> {
                        Person p = ctx.newObject( Person.class );
                        p.setName( "Whoops!" );
                        ctx.commitChanges();
                    } ).start();
                }
            }
        }
    }
}
