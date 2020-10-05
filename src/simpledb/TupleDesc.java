package simpledb;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.NoSuchElementException;

import java.util.Iterator;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc {
    
    public static class TupleDescField implements Serializable{

        /**
         *
         */
        private static final long serialVersionUID = 3282530667679650480L;

        Type field_type;
        String field_name;

        public TupleDescField(Type field_type,String field_name){
            this.field_type=field_type;
            this.field_name=field_name;
        }

        public String toString() {
            return field_name + "(" + field_type + ")";
        }
    }

    List<TupleDescField> tuple_field;
    HashMap<String,Integer> field_hash;


    public TupleDesc(){
        this.tuple_field= new ArrayList<TupleDescField>();
        this.field_hash=new HashMap<String,Integer>();
    }
    
    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields
     * fields, with the first td1.numFields coming from td1 and the remaining
     * from td2.
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc combine(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        TupleDesc tup_desc= new TupleDesc();

        tup_desc.tuple_field.addAll(td1.tuple_field);
        tup_desc.tuple_field.addAll(td2.tuple_field);

        return tup_desc;
    }

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        this.tuple_field= new ArrayList<TupleDescField>();
        this.field_hash=new HashMap<String,Integer>();

        for (int i = 0; i < fieldAr.length; i++) {
            Type field_type=typeAr[i];
            String field_name=fieldAr[i];
            this.addField(field_type, field_name,i);
        }
    }

    /**
     * Constructor.
     * Create a new tuple desc with typeAr.length fields with fields of the
     * specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here

        this.tuple_field= new ArrayList<TupleDescField>();
        this.field_hash=new HashMap<String,Integer>();

        if(typeAr.length > 0)
        
            for (int j=0;j<typeAr.length;j++){

                TupleDescField tuple_desc_field=new TupleDescField(typeAr[j],"");
                this.field_hash.put("", j);
                this.tuple_field.add(tuple_desc_field);
            }
    }

    public void addField(Type field_type,String field_name,int id){
        TupleDescField tuple_desc_field=new TupleDescField(field_type,field_name);
        this.field_hash.put(field_name,id);
        this.tuple_field.add(tuple_desc_field);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        if(this.tuple_field!=null)
            return this.tuple_field.size();
        return 0;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if(i>=0 && i < this.tuple_field.size())
            return this.tuple_field.get(i).field_name;
        return "null";
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int nameToId(String name) throws NoSuchElementException {
        // some code goes here
        if(this.field_hash!=null)
        {
            if(this.field_hash.containsKey(name))
                return this.field_hash.get(name);
            else
                throw new NoSuchElementException("Cannot found element "+name);
        }
        return 0;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getType(int i) throws NoSuchElementException {
        // some code goes here
        if(i > -1 && this.tuple_field!=null)
            return this.tuple_field.get(i).field_type;
        return null;
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size_field=0;

        if(this.tuple_field!=null)
            for (TupleDescField tupleDescField : this.tuple_field) {
                size_field+=tupleDescField.field_type.getLen();
            }

        return size_field;
    }

    /**
     * Compares the specified object with this TupleDesc for equality.
     * Two TupleDescs are considered equal if they are the same size and if the
     * n-th type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object obj) {

        
        if(obj==null || !obj.getClass().equals(this.getClass()))
            return false;

        // some code goes here
        TupleDesc o = (TupleDesc) obj;

        if(this.numFields()!=o.numFields())
            return false;

        for (int i = 0; i < this.tuple_field.size(); i++) {
            if(!this.tuple_field.get(i).field_type.equals(o.tuple_field.get(i).field_type))
                return false;
        }

        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        return this.tuple_field.hashCode();
    }

    public Iterator<TupleDescField> iterator() {
        // return iterator for tuple_field
        return tuple_field.iterator();
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        String descString="";

        if(this.getSize() > 0)
            for(TupleDescField tupe_field: this.tuple_field)
                descString+=tupe_field.field_name;

        return descString;
    }
}
