package fr.upem.net.tcp.http;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class HTTPReader {

	private final SocketChannel sc;
	private final ByteBuffer buff;

	public HTTPReader(SocketChannel sc, ByteBuffer buff) {
		this.sc = sc;
		this.buff = buff;
	}

	/**
	 * @return The ASCII string terminated by CRLF
	 *         <p>
	 *         The method assume that buff is in write mode and leave it in
	 *         write-mode The method never reads from the socket as long as the
	 *         buffer is not empty
	 * @throws IOException
	 *             HTTPException if the connection is closed before a line could
	 *             be read
	 */
	public String readLineCRLF() throws IOException {
		StringBuilder sb = new StringBuilder();
		boolean lastCR = false;
		while (true) {
			buff.flip();
			while (buff.hasRemaining()) {
				char currentChar = (char) buff.get();
				sb.append(currentChar);
				if (currentChar == '\n' && lastCR) {
					sb.setLength(sb.length() - 2);
					buff.compact();
					return sb.toString();
				}
				lastCR = (currentChar == '\r');
			}
			buff.compact();
			if (sc.read(buff) == -1) {
				throw new HTTPException();
			}
		}
	}

	/**
	 * @return The HTTPHeader object corresponding to the header read
	 * @throws IOException
	 *             HTTPException if the connection is closed before a header
	 *             could be read if the header is ill-formed
	 */
	public HTTPHeader readHeader() throws IOException {
		String resp = readLineCRLF();

		String header;
		String save;
		String[] splited = new String[2];
		HashMap<String, String> map = new HashMap<>();
		while (!(header = readLineCRLF()).isEmpty()) {
			splited = header.split(":", 2);
			save = map.put(splited[0], splited[1]);
			if (save != null) {
				map.put(splited[0], new StringBuilder().append(save).append(";").append(splited[1]).toString());
			}
		}
		return HTTPHeader.create(resp, map);
	}

	/**
	 * @param size
	 * @return a ByteBuffer in write-mode containing size bytes read on the
	 *         socket
	 * @throws IOException
	 *             HTTPException is the connection is closed before all bytes
	 *             could be read
	 */
	public ByteBuffer readBytes(int size) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(size);
		buff.flip();
		if (buff.remaining() > 0) {
			buffer.put(buff);
		}
		while (buffer.hasRemaining()) {
			if (sc.read(buffer) == -1) {
				break;
			}
		}
		if (buffer.hasRemaining()) {
			throw new HTTPException();
		}
		return buffer;
	}

	/**
	 * @return a ByteBuffer in write-mode containing a content read in chunks
	 *         mode
	 * @throws IOException
	 *             HTTPException if the connection is closed before the end of
	 *             the chunks if chunks are ill-formed
	 */

	public ByteBuffer readChunks() throws IOException {
		int size, num = 0;
		List<ByteBuffer> liste = new ArrayList<>();
		ByteBuffer buffadd;
		int total = 0;
		while ((size = Integer.parseInt(this.readLineCRLF(), 16)) != 0) {
			buff.flip();
			buffadd = ByteBuffer.allocate(size);
			num = 0;
			if ((num = buff.remaining()) > 0) {
				buffadd.put(buff);
			}
			buff.compact();
			total += size;
			ByteBuffer readBytes = this.readBytes(size - num);
			readBytes.flip();

			buffadd.put(readBytes);
			liste.add(buffadd);
			this.readLineCRLF();
		}
		ByteBuffer buffer = ByteBuffer.allocate(total);
		for (ByteBuffer bb : liste) {
			bb.flip();
			buffer.put(bb);
		}
		return buffer;
	}

	public static void main(String[] args) throws IOException {
		Charset charsetASCII = Charset.forName("ASCII");
		String request = "GET / HTTP/1.1\r\n" + "Host: www.w3.org\r\n" + "\r\n";
		SocketChannel sc = SocketChannel.open();
		sc.connect(new InetSocketAddress("www.w3.org", 80));
		sc.write(charsetASCII.encode(request));
		ByteBuffer bb = ByteBuffer.allocate(50);
		HTTPReader reader = new HTTPReader(sc, bb);
		// System.out.println(reader.readLineCRLF());
		// System.out.println(reader.readLineCRLF());
		// System.out.println(reader.readLineCRLF());
		sc.close();

		bb = ByteBuffer.allocate(50);
		sc = SocketChannel.open();
		sc.connect(new InetSocketAddress("www.w3.org", 80));
		reader = new HTTPReader(sc, bb);
		sc.write(charsetASCII.encode(request));
		// System.out.println(reader.readHeader());
		sc.close();

		bb = ByteBuffer.allocate(50);
		sc = SocketChannel.open();
		sc.connect(new InetSocketAddress("www.w3.org", 80));
		reader = new HTTPReader(sc, bb);
		sc.write(charsetASCII.encode(request));
		HTTPHeader header = reader.readHeader();
		// System.out.println(header);
		ByteBuffer content = reader.readBytes(header.getContentLength());
		content.flip();
		// System.out.println(header.getCharset().decode(content));
		sc.close();

		bb = ByteBuffer.allocate(50);
		request = "GET / HTTP/1.1\r\n" + "Host: www.u-pem.fr\r\n" + "\r\n";
		sc = SocketChannel.open();
		sc.connect(new InetSocketAddress("www.u-pem.fr", 80));
		reader = new HTTPReader(sc, bb);
		sc.write(charsetASCII.encode(request));
		header = reader.readHeader();
		// System.out.println(header);
		content = reader.readChunks();
		content.flip();
		System.out.println(header.getCharset().decode(content));
		sc.close();
	}
}