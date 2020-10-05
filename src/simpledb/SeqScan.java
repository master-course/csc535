package simpledb;
import java.util.*;

import simpledb.TupleDesc.TupleDescField;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    TransactionId tid;
    int tableid;
    String tableAlias;
    private DbFileIterator iterator;
    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid The transaction this scan is running as a part of.
     * @param tableid the table to scan.
     * @param tableAlias the alias of this table (needed by the parser);
     *         the returned tupleDesc should have fields with name tableAlias.fieldName
     *         (note: this class is not responsible for handling a case where tableAlias
     *         or fieldName are null.  It shouldn't crash if they are, but the resulting
     *         name can be null.fieldName, tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.tid=tid;
        this.tableid=tableid;
        this.tableAlias=tableAlias;
    }

    public void open()
        throws DbException, TransactionAbortedException {
        // some code goes here
        DbFile file = Database.getCatalog().getDbFile(tableid);
        iterator = file.iterator(tid);
        iterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     * @return the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc td = Database.getCatalog().getTupleDesc(tableid);
        Iterator<TupleDescField> tdItems = td.iterator();
        Type[] typeAr = new Type[td.numFields()];
        String[] fieldNameAr= new String[td.numFields()];
        int i = 0;
        while(tdItems.hasNext()){
            TupleDescField tdItem = tdItems.next();
            typeAr[i] = tdItem.field_type;
            fieldNameAr[i] = tableAlias + "." + tdItem.field_name;
            i++;
        }
        return new TupleDesc(typeAr, fieldNameAr);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        return iterator.hasNext();  
    }

    public Tuple next()
        throws NoSuchElementException, TransactionAbortedException, DbException {
        // some code goes here
        return iterator.next();  
    }

    public void close() {
        // some code goes here
        iterator.close();
    }

    public void rewind()
        throws DbException, NoSuchElementException, TransactionAbortedException {
        // some code goes here
        iterator.rewind();
    }
}
