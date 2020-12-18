package gofabian.r2dbc.jooq;

import gofabian.r2dbc.jooq.converter.CompositeConverter;
import gofabian.r2dbc.jooq.converter.Converter;
import io.r2dbc.spi.ConnectionFactory;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.r2dbc.dialect.*;
import org.springframework.r2dbc.core.DatabaseClient;

import java.lang.reflect.InvocationTargetException;

@Configuration
public class R2dbcJooqAutoConfiguration {

    @Bean
    public DSLContext dslContext(DatabaseClient databaseClient, ConnectionFactory connectionFactory) {
        R2dbcDialect r2dbcDialect = DialectResolver.getDialect(connectionFactory);
        SQLDialect jooqDialect = translateToJooqDialect(r2dbcDialect);
        DSLContext dslContext = DSL.using(jooqDialect);
        dslContext.configuration().data("databaseClient", databaseClient);
        dslContext.configuration().data("converter", getConverter(jooqDialect));
        return dslContext;
    }

    private SQLDialect translateToJooqDialect(R2dbcDialect r2dbcDialect) {
        if (r2dbcDialect instanceof MySqlDialect) {
            return SQLDialect.MYSQL;
        }
        if (r2dbcDialect instanceof H2Dialect) {
            return SQLDialect.H2;
        }
        if (r2dbcDialect instanceof PostgresDialect) {
            return SQLDialect.POSTGRES;
        }
        throw new IllegalArgumentException("unsupported r2dbc dialect " + r2dbcDialect.getClass());
    }

    private Converter getConverter(SQLDialect sqlDialect) {
        switch (sqlDialect) {
            case POSTGRES:
                return createConverter("gofabian.r2dbc.jooq.converter.PostgresConverter");
            case MYSQL:
            case H2:
            default:
                return new CompositeConverter(new Converter[0]);
        }
    }

    private Converter createConverter(String className) {
        try {
            return (Converter) Class.forName(className).getConstructor().newInstance();
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new RuntimeException("Cannot create instance of '" + className + "'");
        }
    }

}
