package gofabian.db;

import org.jooq.TableField;
import org.jooq.UniqueKey;
import org.jooq.impl.DSL;
import org.jooq.impl.Internal;
import org.jooq.impl.SQLDataType;
import org.jooq.impl.TableImpl;

import java.util.Collections;
import java.util.List;

public class BookTable extends TableImpl<BookRecord> {

    public static final BookTable BOOK_TABLE = new BookTable();

    public static final UniqueKey<BookRecord> ID_PKEY = Internal.createUniqueKey(BOOK_TABLE, "id_pkey", BOOK_TABLE.ID);

    public final TableField<BookRecord, Long> ID = createField(DSL.name("id"), SQLDataType.BIGINT, this, "");
    public final TableField<BookRecord, String> NAME = createField(DSL.name("name"), SQLDataType.VARCHAR, this, "");

    public BookTable() {
        super(DSL.name("book"), null);
    }

    @Override
    public UniqueKey<BookRecord> getPrimaryKey() {
        return ID_PKEY;
    }

    @Override
    public List<UniqueKey<BookRecord>> getKeys() {
        return Collections.singletonList(ID_PKEY);
    }

    @Override
    public Class<? extends BookRecord> getRecordType() {
        return BookRecord.class;
    }

}
