package org.ptg.quarkdb;

import gnu.trove.list.linked.TIntLinkedList;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.lookup.MapLookup;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;

import com.kamikaze.docidset.api.DocSet;
import com.kamikaze.docidset.impl.AndDocIdSet;
import com.kamikaze.docidset.impl.PForDeltaDocIdSet;

public class JavaQuarkDB {
	private static final Logger logger = LogManager.getLogger(JavaQuarkDB.class);
	private TIntObjectHashMap<String> r = new TIntObjectHashMap();
	private TObjectIntHashMap<String> f = new TObjectIntHashMap();
	private int count = 0;
	private int inserStart = 100000;
	private String splitStringConst = ";;\n\t ";
	private String name;

	public JavaQuarkDB(int i) {
		this.inserStart = i;
	}

	public void flush() throws Exception {
		long t1 = System.currentTimeMillis();
		File rfile = new File("data/" + this.name + "r.out");
		if (rfile.exists()) {
			rfile.delete();
		}
		ObjectOutputStream rstream = new ObjectOutputStream(new FileOutputStream(rfile));
		this.r.writeExternal(rstream);
		rstream.flush();
		rstream.close();
		long t2 = System.currentTimeMillis();
		logger.info("Time taken to flush : " + (t2 - t1));
	}

	public void loadSerialized() throws Exception {
		File rfile = new File("data/" + this.name + "r.out");
		TIntObjectHashMap rnew = new TIntObjectHashMap();
		TObjectIntHashMap fnew = new TObjectIntHashMap();
		if (rfile.exists()) {
			ObjectInputStream rstream = new ObjectInputStream(new FileInputStream(rfile));
			rnew.readExternal(rstream);
			rstream.close();
			int maxInDb = -1;
			int[] keys = rnew.keys();
			if (keys != null) {
				maxInDb = NumberUtils.max(keys);
			}

			logger.info("Max id in db: " + maxInDb + ", setting counter to : " + this.inserStart);
			for (int k : keys) {
				fnew.put(rnew.get(k), k);
			}
		} else {
			logger.info(rfile.getAbsolutePath() + " is missing not loading rdb");
		}

		this.r = rnew;
		this.f = fnew;
	}

	public int[] simpleCompact() {
		long t1 = System.currentTimeMillis();

		TIntLinkedList intlist = new TIntLinkedList();
		TIntObjectHashMap rnew = new TIntObjectHashMap();
		TObjectIntHashMap fnew = new TObjectIntHashMap();
		int maxInDb = NumberUtils.max(this.r.keys());
		int current = 1;
		for (int i = current; i <= maxInDb + 1; i++) {
			String s = this.r.get(i);
			if (s != null) {
				if (i != current) {
					intlist.add(i);
					intlist.add(current);
				}
				rnew.put(current, s);
				fnew.put(s, current);
				current++;
			}
		}
		this.r = null;
		this.f = null;
		this.r = rnew;
		this.f = fnew;
		int[] array = intlist.toArray();
		long t2 = System.currentTimeMillis();
		logger.info("Time taken to compact : " + (t2 - t1));
		return array;
	}

	public void loadDB(String fileNameToLoad) throws Exception {
		long t1 = System.currentTimeMillis();
		List<String> lines = IOUtils.readLines(new BufferedInputStream(new FileInputStream(fileNameToLoad)));
		for (String ln : lines) {
			this.count += 1;
			putString(ln, this.count);
		}
		long t3 = System.currentTimeMillis();
		logger.info("Time to load db: " + (t3 - t1) / 1000L);
	}

	public void startCmdLineServer() throws Exception {
		Scanner sc = new Scanner(System.in);
		String read = null;
		while (!(read = sc.next()).equals("!STOP")) {
			if (read.equals("!LIST")) {
				logger.info("!LIST");
				int[] keys = this.r.keys();
				Arrays.sort(keys);
				for (int i : keys) {
					System.out.println(i + "," + this.r.get(i));
				}
			} else if (read.equals("!COMPACT")) {
				logger.info("!COMPACT");
				int[] ar = simpleCompact();
				for (int i = 0; i < ar.length - 1; i += 2) {
					System.out.println(ar[i] + "," + ar[i + 1]);
				}
			} else if (read.equals("!FLUSH")) {
				logger.info("!FLUSH");
				flush();
			} else {
				int val = atoi(read);
				System.out.print(val);
				logger.debug("found : {}  at {}", new Object[] { read, Integer.valueOf(val) });
			}
		}
	}

	public String itoa(int val) {
		return this.r.get(val);
	}

	public int atoi(String read) {
		int val = this.f.get(read);
		if (val < 1) {
			this.inserStart += 1;
			val = this.inserStart;
			putString(read, val);
		}
		return val;
	}

	public void putString(String read, int count) {
		this.f.put(read, count);
		this.r.put(count, read);
	}

	public int[] processString(String read) {
		int[] longs = null;
		String[] splits = StringUtils.split(read, this.splitStringConst);
		longs = new int[splits.length];
		int i = 0;
		for (String s : splits) {
			longs[i] = atoi(s);
			i++;
		}
		return longs;
	}

	public static void main(String[] args) throws Exception {
		MapLookup.setMainArguments(args);
		logger.info("Started");
		JavaQuarkDB d = new JavaQuarkDB(Integer.parseInt(args[0]));
		d.setName(args[1]);
		d.loadSerialized();
		d.startudpServer(Integer.parseInt(args[2]));

		d.startCmdLineServer();
	}

	public String getSplitStringConst() {
		return this.splitStringConst;
	}

	public void setSplitStringConst(String splitStringConst) {
		this.splitStringConst = splitStringConst;
	}

	public DocIdSet addDocIds(ArrayList<Integer> Ids) throws Exception {
		DocSet pForDeltaDocSet = new PForDeltaDocIdSet();
		for (Iterator localIterator = Ids.iterator(); localIterator.hasNext();) {
			int Id = ((Integer) localIterator.next()).intValue();
			pForDeltaDocSet.addDoc(Id);
		}
		return pForDeltaDocSet;
	}

	public ArrayList<Integer> getDocIds(PForDeltaDocIdSet pForDeltaDocSet) throws Exception {
		DocIdSetIterator iter = pForDeltaDocSet.iterator();
		ArrayList Ids = new ArrayList();
		int docId = iter.nextDoc();

		while (docId != 2147483647) {
			Ids.add(Integer.valueOf(docId));
			docId = iter.nextDoc();
		}
		return Ids;
	}

	public boolean search(int target, PForDeltaDocIdSet pForDeltaDocSet) {
		return pForDeltaDocSet.find(target);
	}

	public ArrayList<Integer> findAndIntersections(PForDeltaDocIdSet pForDeltaDocSet1, PForDeltaDocIdSet pForDeltaDocSet2, PForDeltaDocIdSet pForDeltaDocSet3) throws Exception {
		ArrayList docs = new ArrayList();
		docs.add(pForDeltaDocSet1);
		docs.add(pForDeltaDocSet2);
		docs.add(pForDeltaDocSet3);

		ArrayList intersectedIds = new ArrayList();
		AndDocIdSet andSet = new AndDocIdSet(docs);
		DocIdSetIterator iter = andSet.iterator();

		int docId = iter.nextDoc();
		while (docId != 2147483647) {
			intersectedIds.add(Integer.valueOf(docId));
			docId = iter.nextDoc();
		}

		return intersectedIds;
	}

	public String getName() {
		return this.name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void startudpServer(final int port) throws Exception {
		Thread th = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					DatagramSocket serverSocket = new DatagramSocket(port);

					byte[] sendData = new byte[4];
					while (true) {
						byte[] receiveData = new byte[1024];
						DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
						serverSocket.receive(receivePacket);
						byte[] str = new byte[receivePacket.getLength()];
						System.arraycopy(receiveData, 0, str, 0, str.length);
						String word = new String(str);
						JavaQuarkDB.logger.debug("RECEIVED: " + word);
						InetAddress IPAddress = receivePacket.getAddress();
						int outport = receivePacket.getPort();
						int quark = JavaQuarkDB.this.atoi(word);
						JavaQuarkDB.this.itoba(quark, sendData);
						DatagramPacket sendPacket = new DatagramPacket(sendData, 4, IPAddress, outport);
						serverSocket.send(sendPacket);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
		th.setDaemon(true);
		th.start();
	}

	public void itoba(int i, byte[] b) {
		b[0] = (byte) (i >> 24);
		b[1] = (byte) (i >> 16);
		b[2] = (byte) (i >> 8);
		b[3] = (byte) i;
	}

	public static int fromByteArray(byte[] bytes) {
		return ByteBuffer.wrap(bytes).getInt();
	}

	public static int[] intArrayFromByteArray(byte[] bytes) {
		int count = bytes.length / 4;
		int[] ret = new int[count];
		for (int i = 0; i < count; i++) {
			ret[i] = ByteBuffer.wrap(bytes).getInt(i * 4);
		}
		return ret;
	}
}