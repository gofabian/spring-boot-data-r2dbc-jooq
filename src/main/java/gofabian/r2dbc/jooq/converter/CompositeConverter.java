package gofabian.r2dbc.jooq.converter;

public class CompositeConverter implements Converter {

    private final Converter[] converters;

    public CompositeConverter(Converter[] converters) {
        this.converters = converters;
    }

    @Override
    public Object toJooqValue(Object r2dbcValue, Class<?> targetJooqType) {
        Object value = r2dbcValue;
        for (Converter converter : converters) {
            value = converter.toJooqValue(value, targetJooqType);
        }
        return value;
    }

    @Override
    public Object toR2dbcValue(Object jooqValue) {
        Object value = jooqValue;
        for (Converter converter : converters) {
            value = converter.toR2dbcValue(value);
        }
        return value;
    }

    @Override
    public Class<?> toR2dbcType(Class<?> jooqType) {
        Class<?> type = jooqType;
        for (Converter converter : converters) {
            type = converter.toR2dbcType(type);
        }
        return type;
    }
}
