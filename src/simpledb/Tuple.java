package simpledb;

import java.util.Arrays;

/**
 * Tuple maintains information about the contents of a tuple.
 * Tuples have a specified schema specified by a TupleDesc object and contain
 * Field objects with the data for each field.
 */
public class Tuple {

    private Field[] _listField=null;
    private TupleDesc _tuple_desc;
    private RecordId _record_id;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc
     * instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        this._tuple_desc=td;
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this._tuple_desc;
    }

    /**
     * @return The RecordId representing the location of this tuple on
     *   disk. May be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return this._record_id;
    }

    /**
     * Set the RecordId information for this tuple.
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        this._record_id=rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here

        if (i > -1)

            if(this._listField==null){
                this._listField=new Field[i+1];
            }else{
                this._listField= Arrays.copyOf(this._listField,this._listField.length+i);
            }
            this._listField[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        if(i > -1 )
            return this._listField[i];
        return null;
    }

    /**
     * Returns the contents of this Tuple as a string.
     * Note that to pass the system tests, the format needs to be as
     * follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     *
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
        // some code goes here
        String descrip = "";
        for (int i =0; i< this._listField.length;i++){
            if (this._listField[i]==null){
                descrip+="null"+"\t";
            }else {
                descrip+=this._listField[i].toString()+"\t";

            }
        }
        descrip=descrip.substring(0, descrip.length() - 1) + "\n";
        return descrip;
    }
}
