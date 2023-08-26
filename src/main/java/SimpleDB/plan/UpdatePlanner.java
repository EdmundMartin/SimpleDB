package SimpleDB.plan;

import SimpleDB.parse.*;
import SimpleDB.transaction.Transaction;

public interface UpdatePlanner {

    public int executeInsert(InsertData data, Transaction transaction);

    public int executeDelete(DeleteData data, Transaction transaction);

    public int executeModify(ModifyData data, Transaction transaction);

    public int executeCreateTable(CreateTableData data, Transaction transaction);

    public int executeCreateView(CreateViewData data, Transaction transaction);

    public int executeCreateIndex(CreateIndexData data, Transaction transaction);
}
