package simpledb;
import java.util.*;
import java.io.*;

public class HeapFileDbFileIterator implements DbFileIterator {

	private Iterator<Tuple> i;
	private TransactionId tid;
	private int pno;
	private HeapFile hf;
	
	HeapFileDbFileIterator(TransactionId tid, HeapFile f){
		this.tid=tid;
		this.hf=f;
	}

	@Override
	public void open() throws DbException, TransactionAbortedException {
		pno=0;
		
		i=getTuple(pno).iterator();
		// TODO Auto-generated method stub
		
	}
	@Override
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();

    }

    @Override
    public void close() {
        i = null;

    }

	@Override
	public boolean hasNext() throws DbException, TransactionAbortedException {
		// TODO Auto-generated method stub
		if(i==null)
			return false;
		if(i.hasNext())
			return true;
		else if(pno<hf.numPages()-1){
			if(getTuple(pno+1).size()!=0)
				return true;
			else
				return false;
	
		}
		return false;
	}
					
	

	@Override
	public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
		// TODO Auto-generated method stub
		
		if(i.hasNext()){
			Tuple t=i.next();
			return t;
		}
		else if(i.hasNext()==false && pno<hf.numPages()-1){
			pno=pno+1;
			i=getTuple(pno).iterator();
			if(i.hasNext())
				return i.next();
			else
				throw new NoSuchElementException("Tuples Over");
		}
		else{
			throw new NoSuchElementException("Tuples Over");
			
		}
	}
	private List<Tuple> getTuple(int pno) throws TransactionAbortedException, DbException{
        
		PageId pid = new HeapPageId(hf.getId(), pno);
        Page pg = Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                        
        List<Tuple> tupples = new ArrayList<Tuple>();
        
        Iterator<Tuple> i = ((HeapPage)pg).iterator();
        while(i.hasNext()){
            tupples.add(i.next());
        }
        return  tupples;
            
	}
}
