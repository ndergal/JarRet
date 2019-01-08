package upem.jarret;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import fr.upem.net.tcp.http.HTTPException;

public class ServerJarRet {

	private static class Context {
		private boolean inputClosed = false;
		private final ByteBuffer in = ByteBuffer.allocate(BUF_SIZE);
		private final ByteBuffer out = ByteBuffer.allocate(BUF_SIZE);
		private final SelectionKey key;
		private final SocketChannel sc;
		private long time;
		private boolean header = false;
		private boolean eof = false;
		private StringBuilder sbHeader = new StringBuilder();

		public Context(SelectionKey key) {
			this.key = key;
			this.sc = (SocketChannel) key.channel();
			this.time = 0;
		}

		public void doRead() throws IOException {
			int read = this.sc.read(this.in);
			if (read == -1) {
				this.eof = true;
			}
			System.out.println(read);
			process();
			updateInterestOps();
		}

		public void doWrite() throws IOException {
			this.out.flip();
			this.sc.write(this.out);
			this.out.compact();
			process();
			updateInterestOps();
		}

		private void process() {
			if (!header) {
				tryReadHeader();
			} else {
				if (sbHeader.toString().contains("Task")) {
					System.out.println(sbHeader.toString());
				} else {
					if (sbHeader.toString().contains("Answer")) {
						System.out.println(sbHeader.toString());
					}
				}

			}

		}

		private void tryReadHeader() {
			in.flip();
			while (in.hasRemaining()) {
				char current = (char) in.get();
				sbHeader.append(current);
				if (sbHeader.toString().endsWith("\r\n\r\n")) {
					header = true;
					in.compact();
					return;
				}
			}
			in.compact();
		}

		private void updateInterestOps() {
			int ops = 0;
			if (out.position() != 0) {
				ops |= SelectionKey.OP_WRITE;
			}
			if (in.hasRemaining() && !inputClosed) {
				ops |= SelectionKey.OP_READ;
			}
			if (ops == 0) {
				silentlyClose((SocketChannel) key.channel());
			} else {
				key.interestOps(ops);
			}
		}

		private void resetInactiveTime() {
			this.time = System.currentTimeMillis();
		}

		private void addInactiveTime(long time, long timeout) {
			this.time += time;
			if (time > timeout) {
				silentlyClose(sc);
			}
		}

	}

	private static final int BUF_SIZE = 512;
	private static final long TIMEOUT = 300;
	private final ServerSocketChannel serverSocketChannel;
	private final Selector selector;
	private final Set<SelectionKey> selectedKeys;
	private final BlockingQueue<String> queue = new ArrayBlockingQueue<>(1);
	private final Path log;
	private final Path response;
	private final int maxSize;
	private final int delay;

	private final List<String> tasks = new ArrayList<>();

	public ServerJarRet(int port, Path log, Path response, int maxSize, int delay, Path job) throws IOException {
		serverSocketChannel = ServerSocketChannel.open();
		serverSocketChannel.bind(new InetSocketAddress(port));
		selector = Selector.open();
		selectedKeys = selector.selectedKeys();
		this.log = log;
		this.response = response;
		this.maxSize = maxSize;
		this.delay = delay;
		List<Map<String, String>> jobs = new ArrayList<>();
		Scanner sc = new Scanner(job);
		ArrayList<StringBuilder> sblist = new ArrayList<>();
		StringBuilder sb = new StringBuilder();
		sblist.add(sb);
		while (sc.hasNextLine()) {
			String ligne = sc.nextLine();
			if (ligne.isEmpty()) {
				sb = new StringBuilder();
				sblist.add(sb);
				continue;
			}
			sb.append(ligne);
		}
		sc.close();

		sblist.forEach(u -> {
			try {
				Map<String, String> jsonContentToMap = jsonContentToMap(u.toString());
				jobs.add(jsonContentToMap);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});
		List<Integer> numbers = new ArrayList<>();
		int total = 0;
		for (Map<String, String> map : jobs) {
			int parseInt = Integer.parseInt(map.get("JobTaskNumber"));
			numbers.add(parseInt);
			total += parseInt;
		}

		while (total > 0) {
			for (int i = 0; i < jobs.size(); i++) {
				Map<String, String> task = jobs.get(i);
				Integer nb = numbers.get(i);
				if (nb > 0) {
					String str = "{\"JobId\": \"" + task.get("JobId") + "\",\"WorkerVersion\": \""
							+ task.get("WorkerVersionNumber") + "\",\"WorkerURL\": \"" + task.get("WorkerURL")
							+ "\",\"WorkerClassName\": \"" + task.get("WorkerClassName") + "\",\"Task\":        \""
							+ Integer.toString(nb) + "\"}";
				}
			}
		}
	}

	public void launch() throws IOException {
		serverSocketChannel.configureBlocking(false);
		serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
		Set<SelectionKey> selectedKeys = selector.selectedKeys();
		while (!Thread.interrupted()) {
			// printKeys();
			// System.out.println("Starting select");
			long startLoop = System.currentTimeMillis();
			selector.select(TIMEOUT / 10);
			String command = queue.poll();
			if (command != null) {
				command = command.toUpperCase();
				System.out.println(command);
				switch (command) {
				case "HALT":
					shutdownNow();
					break;
				case "STOP":
					shutdown();
					break;
				case "FLUSH":
					killClient();
					break;
				case "SHOW":
					showInfo();
					break;
				}
			}
			// System.out.println("Select finished");
			// printSelectedKey();
			processSelectedKeys();
			long endLoop = System.currentTimeMillis();
			long timeSpent = endLoop - startLoop;
			updateInactivityKeys(timeSpent);
			selectedKeys.clear();
		}
	}

	private void showInfo() {
		int nb = 0;
		for (SelectionKey key : selector.keys()) {
			Context context = (Context) key.attachment();
			if (context != null) {
				nb++;
			}
		}
		System.out.println("il y a " + nb + " clients");
	}

	private void killClient() throws IOException {
		for (SelectionKey key : selector.keys()) {
			Context context = (Context) key.attachment();
			if (context != null)
				context.sc.close();
		}
	}

	private void shutdown() throws IOException {
		serverSocketChannel.close();

	}

	private void shutdownNow() throws IOException {
		serverSocketChannel.close();
		Thread.currentThread().interrupt();
	}

	public void updateInactivityKeys(long timeSpent) {
		for (SelectionKey key : selector.keys()) {
			Context context = (Context) key.attachment();
			if (context != null)
				context.addInactiveTime(timeSpent, TIMEOUT);
		}
	}

	private void processSelectedKeys() throws IOException {
		for (SelectionKey key : selectedKeys) {
			if (key.isValid() && key.isAcceptable()) {
				doAccept(key);
			}
			try {
				Context cntxt = (Context) key.attachment();
				if (cntxt != null)
					cntxt.resetInactiveTime();
				if (key.isValid() && key.isWritable()) {
					cntxt.doWrite();
				}
				if (key.isValid() && key.isReadable()) {
					cntxt.doRead();
				}
			} catch (IOException e) {
				silentlyClose(key.channel());
			}
		}
	}

	private void doAccept(SelectionKey key) throws IOException {
		SocketChannel sc = serverSocketChannel.accept();
		sc.configureBlocking(false);
		SelectionKey clientKey = sc.register(selector, SelectionKey.OP_READ);
		clientKey.attach(new Context(clientKey));
	}

	private static void silentlyClose(SelectableChannel sc) {
		if (sc == null)
			return;
		try {
			sc.close();
		} catch (IOException e) {
			// silently ignore
		}
	}

	private static void usage() {
		System.out.println("ServerSumNew <listeningPort>");
	}

	public static void main(String[] args) throws NumberFormatException, IOException {
		if (args.length != 1) {
			usage();
			return;
		}
		Scanner sc = new Scanner(Paths.get("JarRetConfig.json"));
		StringBuilder sb = new StringBuilder();
		while (sc.hasNextLine()) {
			sb.append(sc.nextLine());

		}
		sc.close();

		String str = sb.toString();
		Map<String, String> map = jsonContentToMap(str);
		System.out.println(map);

		ServerJarRet serv = new ServerJarRet(Integer.parseInt(map.get("Port")), Paths.get(map.get("Log")),
				Paths.get(map.get("ResponseFile")), Integer.parseInt(map.get("MaxSize")),
				Integer.parseInt(map.get("DelayTime")), Paths.get(args[0]));
		serv.startCommandListener(System.in);
		serv.launch();
	}

	static Map<String, String> jsonContentToMap(String ressource) throws IOException {
		HashMap<String, String> request = new HashMap<>();
		JsonFactory factory = new JsonFactory();
		JsonParser parser = null;
		try {
			parser = factory.createParser(ressource);
		} catch (JsonParseException jpe) {
			System.err.println("malformed json");
			return null;
		}
		if (parser.nextToken() != JsonToken.START_OBJECT) {
			System.err.println("json doesn't start");
			return null;
		}

		while (parser.nextToken() != JsonToken.END_OBJECT) {
			String fieldName = parser.getCurrentName();
			parser.nextToken();
			String value = parser.getText();
			request.put(fieldName, value);
		}
		parser.close();
		return request;
	}

	private boolean isJSONWithoutObject(String result) {
		String test = result.substring(1, result.length() - 1);
		return !(test.contains("{") && test.contains("}"));
	}

	private boolean isJSON(String result) {
		JsonFactory factory = new JsonFactory();
		JsonParser parser = null;
		try {
			parser = factory.createParser(result);
			parser.close();
		} catch (JsonParseException jpe) {
			System.err.println("malformed json");
			return false;
		} catch (IOException e) {
			return false;
		}
		return true;
	}

	/***
	 * Theses methods are here to help understanding the behavior of the
	 * selector
	 ***/

	private String interestOpsToString(SelectionKey key) {
		if (!key.isValid()) {
			return "CANCELLED";
		}
		int interestOps = key.interestOps();
		ArrayList<String> list = new ArrayList<>();
		if ((interestOps & SelectionKey.OP_ACCEPT) != 0)
			list.add("OP_ACCEPT");
		if ((interestOps & SelectionKey.OP_READ) != 0)
			list.add("OP_READ");
		if ((interestOps & SelectionKey.OP_WRITE) != 0)
			list.add("OP_WRITE");
		return String.join("|", list);
	}

	public void printKeys() {
		Set<SelectionKey> selectionKeySet = selector.keys();
		if (selectionKeySet.isEmpty()) {
			System.out.println("The selector contains no key : this should not happen!");
			return;
		}
		System.out.println("The selector contains:");
		for (SelectionKey key : selectionKeySet) {
			SelectableChannel channel = key.channel();
			if (channel instanceof ServerSocketChannel) {
				System.out.println("\tKey for ServerSocketChannel : " + interestOpsToString(key));
			} else {
				SocketChannel sc = (SocketChannel) channel;
				System.out.println("\tKey for Client " + remoteAddressToString(sc) + " : " + interestOpsToString(key));
			}
		}
	}

	private String remoteAddressToString(SocketChannel sc) {
		try {
			return sc.getRemoteAddress().toString();
		} catch (IOException e) {
			return "???";
		}
	}

	private void printSelectedKey() {
		if (selectedKeys.isEmpty()) {
			System.out.println("There were not selected keys.");
			return;
		}
		System.out.println("The selected keys are :");
		for (SelectionKey key : selectedKeys) {
			SelectableChannel channel = key.channel();
			if (channel instanceof ServerSocketChannel) {
				System.out.println("\tServerSocketChannel can perform : " + possibleActionsToString(key));
			} else {
				SocketChannel sc = (SocketChannel) channel;
				System.out.println(
						"\tClient " + remoteAddressToString(sc) + " can perform : " + possibleActionsToString(key));
			}

		}
	}

	private String possibleActionsToString(SelectionKey key) {
		if (!key.isValid()) {
			return "CANCELLED";
		}
		ArrayList<String> list = new ArrayList<>();
		if (key.isAcceptable())
			list.add("ACCEPT");
		if (key.isReadable())
			list.add("READ");
		if (key.isWritable())
			list.add("WRITE");
		return String.join(" and ", list);
	}

	private void startCommandListener(InputStream in) {
		new Thread(() -> {
			Scanner sc = new Scanner(in);
			while (sc.hasNextLine()) {
				String str = sc.nextLine();
				try {
					System.out.println(str);
					queue.put(str);
					selector.wakeup();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			sc.close();

		}).start();
	}

}