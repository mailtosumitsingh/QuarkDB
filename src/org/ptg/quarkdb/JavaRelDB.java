package org.ptg.quarkdb;

import gnu.trove.list.linked.TIntLinkedList;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.lookup.MapLookup;

import com.googlecode.javaewah.EWAHCompressedBitmap;

public class JavaRelDB {
	private static final Logger logger = LogManager.getLogger(JavaRelDB.class);
	private TIntObjectHashMap<EWAHCompressedBitmap> r = new TIntObjectHashMap<EWAHCompressedBitmap>();
	private int count = 0;
	private int inserStart = 100000;
	private String splitStringConst = ";;\n\t ";
	private String name;

	public JavaRelDB(int i) {
		this.inserStart = i;
	}

	public void flush() throws Exception {
		long t1 = System.currentTimeMillis();
		File rfile = new File("data/" + name + "r.out");
		if (rfile.exists()) {
			rfile.delete();
		}
		ObjectOutputStream rstream = new ObjectOutputStream(new FileOutputStream(rfile));
		r.writeExternal(rstream);
		rstream.flush();
		rstream.close();
		long t2 = System.currentTimeMillis();
		logger.info("Time taken to flush : " + (t2 - t1));
	}

	public void loadSerialized() throws Exception {
		File rfile = new File("data/" + name + "r.out");
		TIntObjectHashMap<EWAHCompressedBitmap> rnew = new TIntObjectHashMap<EWAHCompressedBitmap>();
		if (rfile.exists()) {
			ObjectInputStream rstream = new ObjectInputStream(new FileInputStream(rfile));
			rnew.readExternal(rstream);
			rstream.close();
			int maxInDb = -1;
			int[] keys = rnew.keys();
			if (keys != null) {
				maxInDb = NumberUtils.max(keys);
			}
			// inserStart = maxInDb + 1;
			logger.info("Max id in db: " + maxInDb + ", setting counter to : " + inserStart);
		} else {
			logger.info(rfile.getAbsolutePath() + " is missing not loading rdb");
		}

		r = rnew;
	}

	public int[] simpleCompact() {
		long t1 = System.currentTimeMillis();
		// example 1 2 5 7 8 9 11
		TIntLinkedList intlist = new TIntLinkedList();
		TIntObjectHashMap<EWAHCompressedBitmap> rnew = new TIntObjectHashMap<EWAHCompressedBitmap>();
		int maxInDb = NumberUtils.max(r.keys());
		int current = 1;
		for (int i = current; i <= maxInDb + 1; i++) {
			EWAHCompressedBitmap s = r.get(i);
			if (s != null) {
				if (i != current) {
					intlist.add(i);
					intlist.add(current);
				}
				rnew.put(current, s);
				current++;
			}
		}
		r = null;
		r = rnew;
		int[] array = intlist.toArray();
		long t2 = System.currentTimeMillis();
		logger.info("Time taken to compact : " + (t2 - t1));
		return array;
	}

	// old->new position
	public void applyCompact(int[] ar) {
		TIntIntHashMap m = new TIntIntHashMap();
		for (int i = 0; i < ar.length - 1; i += 2) {
			m.put(ar[i], ar[i + 1]);
		}
		for (int i = 0; i < ar.length - 1; i += 2) {
			System.out.println(ar[i] + "," + ar[i + 1]);
			if (ar[i] == ar[i + 1]) {
				// ignore same one
				continue;
			}
			EWAHCompressedBitmap orig = r.get(ar[i]);
			if (orig != null) {
				EWAHCompressedBitmap newb = new EWAHCompressedBitmap();
				List<Integer> l = orig.toList();
				for (int idx = 0; idx < l.size(); idx++) {
					int origIdx = l.get(idx);
					int newIdx = m.get(origIdx);
					if (newIdx > 0) {
						// found new postion
						newb.set(newIdx);
					} else {
						newb.set(origIdx);
					}
				}
				r.put(ar[i + 1], newb);
				r.remove(ar[i]);
			}
		}
	}

	public void startCmdLineServer() throws Exception {
		Scanner sc = new Scanner(System.in);
		String read = null;
		while (!(read = sc.next()).equals("!STOP")) {
			if (read.equals("!LIST")) {
				logger.info("!LIST");
				int[] keys = r.keys();
				Arrays.sort(keys);
				for (int i : keys) {
					System.out.println(i + "," + r.get(i));
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
				List<Integer> val = atoi(Integer.parseInt(read));
				for (Integer v : val) {
					System.out.println(v + " ");
				}
				logger.debug("found : {}  at {}", read, val);
			}
		}
	}

	public List<Integer> atoi(int read) {
		EWAHCompressedBitmap val = r.get(read);
		if (val == null) {
			val = new EWAHCompressedBitmap();
			putBitMap(read, val);
		}
		return val.toList();
	}

	public List<Integer> atoiput(int read, EWAHCompressedBitmap valin) {
		EWAHCompressedBitmap val = r.get(read);
		if (val == null) {
			putBitMap(read, valin);
			return valin.toList();
		} else {
			val = val.or(valin);
			putBitMap(read, val);
			return val.toList();
		}

	}

	public EWAHCompressedBitmap atoib(int read) {
		EWAHCompressedBitmap val = r.get(read);
		if (val == null) {
			putBitMap(read, val);
		}
		return val;
	}

	public void putBitMap(int item, EWAHCompressedBitmap b) {
		r.put(item, b);
	}

	public static void main(String[] args) throws Exception {
		MapLookup.setMainArguments(args);
		logger.info("Started");
		JavaRelDB d = new JavaRelDB(Integer.parseInt(args[0]));
		d.setName(args[1]);
		d.loadSerialized();
		d.startudpServer(Integer.parseInt(args[2]));
		d.startReludpServer(Integer.parseInt(args[3]));
		d.startCompationServer(Integer.parseInt(args[4]));
		// d.loadDB(arg[2]);
		d.startCmdLineServer();
	}

	public String getSplitStringConst() {
		return splitStringConst;
	}

	public void setSplitStringConst(String splitStringConst) {
		this.splitStringConst = splitStringConst;
	}

	public String getName() {
		return name;
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
					while (true) {
						byte[] receiveData = new byte[128];
						DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
						serverSocket.receive(receivePacket);
						byte[] realData = new byte[receivePacket.getLength()];
						System.arraycopy(receiveData, 0, realData, 0, realData.length);
						int word = fromByteArray(realData);
						logger.debug("RECEIVED: " + word);
						InetAddress IPAddress = receivePacket.getAddress();
						int outport = receivePacket.getPort();
						EWAHCompressedBitmap quark = atoib(word);
						ByteArrayOutputStream bos = new ByteArrayOutputStream();
						ObjectOutputStream oo = new ObjectOutputStream(bos);
						quark.writeExternal(oo);
						oo.close();
						byte[] sendData = bos.toByteArray();
						DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, outport);
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

	public void startReludpServer(final int port) throws Exception {
		Thread th = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					DatagramSocket serverSocket = new DatagramSocket(port);
					while (true) {
						try {
							byte[] receiveData = new byte[64 * 1024];
							DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
							serverSocket.receive(receivePacket);
							Relation r = new Relation();
							byte[] realreceiveData = new byte[receivePacket.getLength()];
							System.arraycopy(receiveData, 0, realreceiveData, 0, realreceiveData.length);
							ByteArrayInputStream bis = new ByteArrayInputStream(realreceiveData, 0, realreceiveData.length);
							ObjectInputStream ii = new ObjectInputStream(bis);
							r.readExternal(ii);
							logger.debug("RECEIVED: " + r.getIdx());
							atoiput(r.getIdx(), r.getOuts());
							InetAddress IPAddress = receivePacket.getAddress();
							int outport = receivePacket.getPort();
							byte[] sendData = new byte[4];
							itoba(r.getIdx(), sendData);
							DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, outport);
							serverSocket.send(sendPacket);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
		th.setDaemon(true);
		th.start();

	}

	public void startCompationServer(final int port) throws Exception {
		Thread th = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					DatagramSocket serverSocket = new DatagramSocket(port);
					while (true) {
						try {
							byte[] receiveData = new byte[64 * 1024];
							DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
							serverSocket.receive(receivePacket);
							Compaction r = new Compaction();
							byte[] realreceiveData = new byte[receivePacket.getLength()];
							System.arraycopy(receiveData, 0, realreceiveData, 0, realreceiveData.length);
							ByteArrayInputStream bis = new ByteArrayInputStream(realreceiveData, 0, realreceiveData.length);
							ObjectInputStream ii = new ObjectInputStream(bis);
							r.readExternal(ii);
							logger.debug("RECEIVED: " + r.getArr().length);
							applyCompact(r.getArr());
							InetAddress IPAddress = receivePacket.getAddress();
							int outport = receivePacket.getPort();
							byte[] sendData = new byte[4];
							itoba(r.getArr().length, sendData);
							DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, outport);
							serverSocket.send(sendPacket);
						} catch (Exception e) {
							e.printStackTrace();
						}
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
}
