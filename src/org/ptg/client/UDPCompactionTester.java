package org.ptg.client;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

import org.apache.commons.lang.math.RandomUtils;
import org.ptg.quarkdb.Compaction;
import org.ptg.quarkdb.JavaRelDB;

public class UDPCompactionTester {
	public static void main(String args[]) throws Exception {
		BufferedReader inFromUser = new BufferedReader(new InputStreamReader(System.in));
		while (true) {
			DatagramSocket clientSocket = new DatagramSocket();
			InetAddress IPAddress = InetAddress.getByName("localhost");
			byte[] receiveData = new byte[1024];
			String sentence = inFromUser.readLine();
			//
			Compaction r = new Compaction();
			int[] a = new int[10];
			for (int i = 0; i < 10; i += 2) {
				a[i] = RandomUtils.nextInt(20);
				a[i + 1] = RandomUtils.nextInt(20);
			}
			r.setArr(a);
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream oo = new ObjectOutputStream(bos);
			r.writeExternal(oo);
			oo.flush();
			bos.flush();
			//
			byte[] sendData = bos.toByteArray();
			DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, 4002);
			clientSocket.send(sendPacket);
			DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
			clientSocket.receive(receivePacket);
			int idx = JavaRelDB.fromByteArray(receivePacket.getData());
			System.out.println("FROM SERVER:" + idx);
			clientSocket.close();
		}
	}
}