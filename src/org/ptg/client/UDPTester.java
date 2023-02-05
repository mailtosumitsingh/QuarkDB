package org.ptg.client;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Scanner;

import org.ptg.quarkdb.JavaQuarkDB;

public class UDPTester {
	public static void main(String args[]) throws Exception {
		Scanner sc = new Scanner(System.in);
		String read = null;
		while (!(read = sc.next()).equals("!STOP")) {
			DatagramSocket clientSocket = new DatagramSocket();
			InetAddress IPAddress = InetAddress.getByName("192.168.1.11" /* "localhost" */);
			byte[] receiveData = new byte[1024];
			String sentence = read;
			byte[] sendData = sentence.getBytes();
			DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, 4000);
			clientSocket.send(sendPacket);
			DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
			clientSocket.receive(receivePacket);
			int modifiedSentence = JavaQuarkDB.fromByteArray(receivePacket.getData());
			System.out.println("FROM SERVER:" + modifiedSentence);
			clientSocket.close();
		}
	}
}