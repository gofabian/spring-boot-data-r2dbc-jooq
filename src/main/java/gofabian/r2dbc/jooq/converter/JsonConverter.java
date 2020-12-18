package gofabian.r2dbc.jooq.converter;

import org.jooq.JSON;
import org.jooq.JSONB;

public class JsonConverter implements Converter {

    @Override
    public Object toJooqValue(Object r2dbcValue, Class<?> targetJooqType) {
        // keep String type
        return r2dbcValue;
    }

    @Override
    public Object toR2dbcValue(Object jooqValue) {
        if (jooqValue instanceof JSON) {
            return ((JSON) jooqValue).data();
        }
        if (jooqValue instanceof JSONB) {
            return ((JSONB) jooqValue).data();
        }
        return jooqValue;
    }

    @Override
    final public Class<?> toR2dbcType(Class<?> sourceJooqType) {
        if (sourceJooqType == JSON.class || sourceJooqType == JSONB.class) {
            return String.class;
        }
        return sourceJooqType;
    }

}
