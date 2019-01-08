package fr.upem.net.tcp.http;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.Map;

public class HTTPClient {
	public static Charset charsetASCII = Charset.forName("ASCII");
	private String server;
	private int port;

	public String getServer() {
		return server;
	}

	public void setServer(String server) {
		this.server = server;
	}

	public int getPort() {
		return port;
	}

	public HTTPClient(String server, int port) {
		this.server = server;
		this.port = port;
	}

	public String askForTask() throws IOException {
		String request = "GET Task HTTP/1.1\r\n" + "Host: " + server + "\r\n\r\n";
		SocketChannel socketChannel = SocketChannel.open();
		socketChannel.connect(new InetSocketAddress(server, port));
		int write = socketChannel.write(charsetASCII.encode(request));
		System.out.println(write);
		ByteBuffer bb = ByteBuffer.allocate(50);
		HTTPReader reader = new HTTPReader(socketChannel, bb);
		HTTPHeader header = reader.readHeader();
		System.out.println(header.getResponse());
		if (!header.getResponse().equals("HTTP/1.1 200 OK")) {
			System.err.println("wrong response " + server + port);
			return null;
		}
		int size = header.getContentLength();
		ByteBuffer content = null;
		if (size != -1) {
			content = reader.readBytes(size);
			content.flip();
		} else {
			Map<String, String> map = header.getFields();
			if (map.get("Transfer-Encoding").equals("chunked")) {
				content = reader.readChunks();
				content.flip();
			}
		}
		return header.getCharset().decode(content).toString();
	}

	public String sendAnswer(Map<String, String> request, ByteBuffer bb) throws IOException {
		int length = bb.remaining();
		SocketChannel socketChannel = SocketChannel.open();
		socketChannel.connect(new InetSocketAddress(server, port));
		String answer = "POST Answer HTTP/1.1\r\n" + "Host: " + this.getServer() + "\r\n"
				+ "Content-Type: application/json" + "\r\n" + "Content-Length: " + length + "\r\n" + "\r\n";
		ByteBuffer bbreader = ByteBuffer.allocate(50);
		HTTPReader reader = new HTTPReader(socketChannel, bbreader);
		ByteBuffer bbrequest = Charset.forName("utf-8").encode(answer);
		socketChannel.write(bbrequest);
		bbrequest = ByteBuffer.allocate(50);
		bbrequest.putLong(Long.parseLong(request.get("JobId")));
		bbrequest.putInt(Integer.parseInt(request.get("Task")));
		bbrequest.flip();
		socketChannel.write(bbrequest);
		socketChannel.write(bb);
		HTTPHeader header = reader.readHeader();
		return header.getResponse();
	}

}
