package simpledb;

import java.io.IOException;

/**
 * The delete operator.  Delete reads tuples from its child operator and
 * removes them from the table they belong to.
 */
public class Delete extends AbstractDbIterator {

	
	TransactionId tid;
	DbIterator child;
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * @param t The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
    	this.tid=t;
    	this.child=child;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return	child.getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.open();
    }

    public void close() {
        // some code goes here
    	child.close();
 
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	Tuple t=new Tuple(getTupleDesc());
    	int numberOfTuplesDeleted=0;
    	try{
    		while(child.hasNext()){
        		t=child.next();
        		Database.getBufferPool().deleteTuple(tid, t);
        		numberOfTuplesDeleted++;
        	}
    	}catch(DbException db){
    		db.printStackTrace();
    	}
    	TupleDesc td=new TupleDesc(new Type[1]);
    	Tuple count=new Tuple(td);
    	count.setField(0,new IntField(numberOfTuplesDeleted));
    	return count;
    }
}
