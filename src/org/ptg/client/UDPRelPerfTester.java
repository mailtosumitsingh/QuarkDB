package org.ptg.client;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

import org.apache.commons.lang.math.RandomUtils;
import org.ptg.quarkdb.JavaRelDB;
import org.ptg.quarkdb.Relation;

public class UDPRelPerfTester {
	public static void main(String args[]) throws Exception {
		{
			for (int j = 0; j < 1000; j++) {
				DatagramSocket clientSocket = new DatagramSocket();
				InetAddress IPAddress = InetAddress.getByName("localhost");
				byte[] receiveData = new byte[1024];
				//
				Relation r = new Relation();
				r.setIdx(RandomUtils.nextInt(20000) + 1);
				for (int i = 0; i < 1000; i++) {
					r.addRel(RandomUtils.nextInt(20000) + 1);
				}
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				ObjectOutputStream oo = new ObjectOutputStream(bos);
				r.writeExternal(oo);
				oo.flush();
				bos.flush();
				//
				byte[] sendData = bos.toByteArray();
				DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, 4001);
				clientSocket.send(sendPacket);
				DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
				clientSocket.receive(receivePacket);
				int idx = JavaRelDB.fromByteArray(receivePacket.getData());
				System.out.println("FROM SERVER:" + idx);
				clientSocket.close();
			}
		}
	}
}