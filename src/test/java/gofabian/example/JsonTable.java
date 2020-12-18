package gofabian.example;

import org.jooq.Identity;
import org.jooq.TableField;
import org.jooq.UniqueKey;
import org.jooq.impl.DSL;
import org.jooq.impl.Internal;
import org.jooq.impl.SQLDataType;
import org.jooq.impl.TableImpl;

import java.util.Collections;
import java.util.List;

import static org.jooq.impl.Internal.createIdentity;

public class JsonTable extends TableImpl<JsonRecord> {

    public static final JsonTable JSON_TABLE = new JsonTable();

    public static final UniqueKey<JsonRecord> ID_PKEY = Internal.createUniqueKey(JSON_TABLE, "id_pkey", JSON_TABLE.ID);

    public final TableField<JsonRecord, Long> ID = createField(DSL.name("id"), SQLDataType.BIGINT.nullable(false).identity(true), this, "");
    public final TableField<JsonRecord, org.jooq.JSON> JSON = createField(DSL.name("json"), SQLDataType.JSON, this, "");
    public final TableField<JsonRecord, org.jooq.JSONB> JSONB = createField(DSL.name("jsonb"), SQLDataType.JSONB, this, "");

    public JsonTable() {
        super(DSL.name("json"), null);
    }

    @Override
    public Identity<JsonRecord, ?> getIdentity() {
        return createIdentity(this, ID);
    }

    @Override
    public UniqueKey<JsonRecord> getPrimaryKey() {
        return ID_PKEY;
    }

    @Override
    public List<UniqueKey<JsonRecord>> getKeys() {
        return Collections.singletonList(ID_PKEY);
    }

    @Override
    public Class<? extends JsonRecord> getRecordType() {
        return JsonRecord.class;
    }

}
