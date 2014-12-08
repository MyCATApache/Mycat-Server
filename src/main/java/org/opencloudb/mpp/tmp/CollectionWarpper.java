package org.opencloudb.mpp.tmp;

import java.util.AbstractList;
import java.util.Iterator;

import org.opencloudb.net.mysql.RowDataPacket;

/***
 * only support iterator
 * 
 * @author czp:2014年12月8日
 *
 */
public class CollectionWarpper extends AbstractList<RowDataPacket> {

	private int fieldCount;
	private MemMapBytesArray arr;

	public CollectionWarpper(MemMapBytesArray arr, int fieldCount) {
		this.arr = arr;
		this.fieldCount = fieldCount;
	}

	@Override
	public RowDataPacket get(int index) {
		byte[] bs = arr.get(index);
		RowDataPacket pack = new RowDataPacket(fieldCount);
		pack.read(bs);
		return pack;
	}

	@Override
	public int size() {
		return arr.size();
	}

	@Override
	public Iterator<RowDataPacket> iterator() {
		return new Iterator<RowDataPacket>() {
			private Iterator<byte[]> it = arr.iterator();

			@Override
			public RowDataPacket next() {
				byte[] bs = it.next();
				RowDataPacket pack = new RowDataPacket(fieldCount);
				pack.read(bs);
				return pack;
			}

			@Override
			public boolean hasNext() {
				return it.hasNext();
			}

			// @Override
			public void remove() {
				// throw new UnsupportedOperationException();
			}
		};
	}

}
