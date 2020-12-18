package gofabian.r2dbc.jooq.converter;

public class PostgresConverter extends CompositeConverter {
    public PostgresConverter() {
        super(new Converter[]{
                new PostgresJsonConverter(),
                new PostgresJsonbConverter()
        });
    }
}
