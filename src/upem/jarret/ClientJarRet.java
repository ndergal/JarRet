package upem.jarret;

import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import fr.upem.net.tcp.http.HTTPClient;
import upem.jarret.worker.Worker;
import upem.jarret.worker.WorkerFactory;

public class ClientJarRet {

	private int clientID;
	private HTTPClient httpClient;
	private Map<Long, Worker> workers = new HashMap<>();

	public ClientJarRet(int clientID, String server, int port) throws IOException {
		this.clientID = clientID;
		this.httpClient = new HTTPClient(server, port);
	}

	public static void usage() {
		System.err.println("Wrong number of arguments, usage : clientID server address server port ");
	}
	


	private Map<String, String> jsonContentToMap(String ressource) throws IOException {
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

	private Worker getWorker(Map<String, String> request)
			throws MalformedURLException, ClassNotFoundException, IllegalAccessException, InstantiationException {
		long jobId = Long.parseLong(request.get("JobId"));
		String version = request.get("WorkerVersion");
		Worker worker = workers.get(jobId);
		if (worker != null && worker.getVersion().equals(version)) {
			return worker;
		}
		worker = WorkerFactory.getWorker(request.get("WorkerURL"), request.get("WorkerClassName"));
		workers.put(jobId, worker);
		return worker;
	}

	private String taskReply(Map<String, String> request) {
		Worker worker;
		String result;
		try {
			worker = this.getWorker(request);
			result = worker.compute(Integer.parseInt(request.get("Task")));
			if (result == null) {
				return this.errorReply(request, "Computation error");
			}

		} catch (Exception e1) {
			System.err.println("worker error");
			return this.errorReply(request, "Computation error");
		}
		if (!isJSON(result)) {
			return this.errorReply(request, "Answer is not valid JSON");
		}
		if (!isJSONWithoutObject(result)) {
			return this.errorReply(request, "Answer is nested");
		}
		return answerReply(request, result);
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

	private String sendReply(Map<String, String> request, String response) throws IOException {
		ByteBuffer bb = Charset.forName("utf-8").encode(response);
		int length = bb.remaining();
		int lengthHeader = Charset.forName("utf-8")
				.encode("POST Answer HTTP/1.1\r\n" + "Host: " + this.httpClient.getServer() + "\r\n"
						+ "Content-Type: application/json" + "\r\n" + "Content-Length: " + length + "\r\n" + "\r\n")
				.remaining();
		lengthHeader += length;
		if (lengthHeader > 4096) {
			response = errorReply(request, "Too Long");
			bb = Charset.forName("utf-8").encode(response);
			length = bb.remaining();
		}
		return this.httpClient.sendAnswer(request, bb);
	}

	private String errorReply(Map<String, String> request, String string) {
		return "{" + "\"JobId\": \"" + request.get("JobId") + "\"," + "\"WorkerVersion\": \""
				+ request.get("WorkerVersion") + "\"," + "\"WorkerURL\": \"" + request.get("WorkerURL") + "\","
				+ "\"WorkerClassName\": \"" + request.get("WorkerClassName") + "\"," + "\"Task\":        \""
				+ request.get("Task") + "\"," + "\"ClientId\": \"" + this.clientID + "\"," + "\"Error\" : \"" + string
				+ "\"" + "}";
	}

	private String answerReply(Map<String, String> request, String string) {
		return "{" + "\"JobId\": \"" + request.get("JobId") + "\"," + "\"WorkerVersion\": \""
				+ request.get("WorkerVersion") + "\"," + "\"WorkerURL\": \"" + request.get("WorkerURL") + "\","
				+ "\"WorkerClassName\": \"" + request.get("WorkerClassName") + "\"," + "\"Task\":        \""
				+ request.get("Task") + "\"," + "\"ClientId\": \"" + this.clientID + "\"," + "\"Answer\" : " + string
				+ "}";
	}

	public static void main(String[] args) throws IOException {
		if (args.length != 3) {
			usage();
			return;
		}
		ClientJarRet client = new ClientJarRet(Integer.parseInt(args[0]), args[1], Integer.parseInt(args[2]));
		client.start();
	}

	public void start() {
		while (true) {
			String ressource;
			Map<String, String> request = null;
			try {
				ressource = this.httpClient.askForTask();
				if (ressource == null) {
					System.err.println("Protocol Error: The answer to GET Task is not 200");
					return;
				}
				request = this.jsonContentToMap(ressource);
				if (request == null) {
					System.err.println("Protocol Error: The answer to GET Task is invalid :" + ressource);
					return;
				}

				if (request.keySet().equals(new HashSet<>(Arrays.asList(new String[] { "ComeBackInSeconds" })))) {
					System.out.println("wait");
					String time = request.get("ComeBackInSeconds");
					try {
						Thread.sleep(1000 * Integer.parseInt(time));
					} catch (NumberFormatException e) {
						System.err.println("NaN");
						continue;
					} catch (InterruptedException e) {
						throw new AssertionError();
					}
					continue;
				}

				if (request.keySet().equals(new HashSet<>(Arrays
						.asList(new String[] { "JobId", "WorkerVersion", "WorkerURL", "WorkerClassName", "Task" })))) {
					String reply = this.taskReply(request);
					System.out.println(sendReply(request, reply));

				}
			} catch (IOException e1) {
				System.err.println("reconnexion");
				continue;
			}
		}
	}

}
