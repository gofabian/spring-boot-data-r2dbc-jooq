package gofabian.r2dbc.jooq.converter;

import io.r2dbc.postgresql.codec.Json;
import org.jooq.JSONB;

public class PostgresJsonbConverter extends AbstractConverter<JSONB, Json> {

    public PostgresJsonbConverter() {
        super(JSONB.class, Json.class);
    }

    @Override
    protected JSONB toTypedJooqValue(Json r2dbcValue) {
        return JSONB.valueOf(r2dbcValue.asString());
    }

    @Override
    protected Json toTypedR2dbcValue(JSONB jooqValue) {
        return Json.of(jooqValue.data());
    }

}
