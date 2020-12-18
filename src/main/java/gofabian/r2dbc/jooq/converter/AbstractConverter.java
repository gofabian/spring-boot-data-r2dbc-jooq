package gofabian.r2dbc.jooq.converter;

public abstract class AbstractConverter<J, R> implements Converter {

    private final Class<J> jooqType;
    private final Class<R> r2dbcType;

    public AbstractConverter(Class<J> jooqType, Class<R> r2dbcType) {
        this.jooqType = jooqType;
        this.r2dbcType = r2dbcType;
    }

    @Override
    final public Object toJooqValue(Object r2dbcValue, Class<?> targetJooqType) {
        if (r2dbcType.isInstance(r2dbcValue) && targetJooqType.isAssignableFrom(jooqType)) {
            //noinspection unchecked
            return toTypedJooqValue((R) r2dbcValue);
        }
        return r2dbcValue;
    }

    abstract protected J toTypedJooqValue(R r2dbcValue);

    @Override
    final public Object toR2dbcValue(Object jooqValue) {
        if (jooqType.isInstance(jooqValue)) {
            //noinspection unchecked
            return toTypedR2dbcValue((J) jooqValue);
        }
        return jooqValue;
    }

    abstract protected R toTypedR2dbcValue(J jooqValue);

    @Override
    final public Class<?> toR2dbcType(Class<?> sourceJooqType) {
        if (this.jooqType.isAssignableFrom(sourceJooqType)) {
            return r2dbcType;
        }
        return sourceJooqType;
    }

}
