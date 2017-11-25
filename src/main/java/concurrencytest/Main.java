package concurrencytest;

import org.apache.cayenne.ObjectContext;
import org.apache.cayenne.commitlog.CommitLogListener;
import org.apache.cayenne.commitlog.CommitLogModule;
import org.apache.cayenne.commitlog.model.ChangeMap;
import org.apache.cayenne.configuration.server.ServerRuntime;
import org.apache.cayenne.configuration.server.ServerRuntimeBuilder;
import org.apache.cayenne.query.ObjectSelect;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import concurrencytest.data.Person;

public class Main {

    /**
     * Setting this to 'true' will make the CommitLogListener fail with: Transaction must have 'STATUS_ACTIVE' to add a connection. Current status: STATUS_COMMITTED Everything works fine when using the build in Cayenne pool
     */
    private static final boolean useHikariCP = true;

    private static ServerRuntime _serverRuntime;

    private static ServerRuntime serverRuntime() {
        if( _serverRuntime == null ) {
            ServerRuntimeBuilder srtBuilder = ServerRuntime.builder()
                    .addConfig( "cayenne-concurrencytest.xml" )
                    .addModule( CommitLogModule.extend().addListener( AfterUpdateListener.class ).module() );

            if( useHikariCP ) {
                HikariConfig config = new HikariConfig();
                config.setJdbcUrl( "jdbc:h2:mem:concurrencytest" );
                config.setDriverClassName( "org.h2.Driver" );
                srtBuilder = srtBuilder.dataSource( new HikariDataSource( config ) );
            }

            _serverRuntime = srtBuilder.build();
        }

        return _serverRuntime;
    }

    public static void main( String[] args ) {
        Person person = serverRuntime().newContext().newObject( Person.class );
        person.getObjectContext().commitChanges();
    }

    public static class AfterUpdateListener implements CommitLogListener {

        @Override
        public void onPostCommit( ObjectContext originatingContext, ChangeMap changes ) {
            new Thread( () -> {
                ObjectSelect
                        .query( Person.class )
                        .select( serverRuntime().newContext() );
            }, "afterUpdateThread" ).start();
        }
    }
}
