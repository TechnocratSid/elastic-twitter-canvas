package com.technocratsid.elastic;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class Application {

	private final static String consumerKey = "xxxxxxxxxxxxxxxxxx";
	private final static String consumerSecret = "xxxxxxxxxxxxxxxxxx";
	private final static String token = "xxxxxxxxxxxxxxxxxx";
	private final static String secret = "xxxxxxxxxxxxxxxxxx";
	private static Logger logger = Logger.getLogger(Application.class.getName());

	public static void main(String[] args) {
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
		List<String> terms = Lists.newArrayList("tea", "coffee", "beer");

		// Elasticsearch Transport Client
		TransportClient elasticClient = createElasticTransportClient();

		// Twitter HoseBird Client
		Client client = createTwitterClient(msgQueue, terms);
		
		// establish a connection
		client.connect();

		String msg = null;
		int count = 0;

		// Streaming 1000 tweets
		while (!client.isDone() && count != 1000) {
			try {
				msg = msgQueue.take();
				logger.log(Level.INFO, msg);

				// Segregating the tweets
				if (msg.contains(" tea ")) {
					insertIntoElastic(elasticClient, "tea");
				} else if (msg.contains(" coffee ")) {
					insertIntoElastic(elasticClient, "coffee");
				} else {
					insertIntoElastic(elasticClient, "beer");
				}
				count++;
			} catch (InterruptedException ex) {
				logger.log(Level.SEVERE, ex.getMessage());
				client.stop();
			}
		}
		
		// Closing the clients 
		client.stop();
		elasticClient.close();
	}

	public static Client createTwitterClient(BlockingQueue<String> msgQueue, List<String> terms) {
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		hosebirdEndpoint.trackTerms(terms); // tweets with the specified terms
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);
		ClientBuilder builder = new ClientBuilder().name("Twitter-Elastic-Client").hosts(hosebirdHosts)
				.authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));
		Client hosebirdClient = builder.build();
		return hosebirdClient;
	}

	@SuppressWarnings("resource")
	public static TransportClient createElasticTransportClient() {
		TransportClient client = null;
		try {
			client = new PreBuiltTransportClient(Settings.EMPTY)
					.addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));
		} catch (UnknownHostException ex) {
			logger.log(Level.SEVERE, ex.getMessage());
		}
		return client;
	}

	public static void insertIntoElastic(TransportClient client, String tweet) {
		try {
			client.prepareIndex("drink-popularity", "_doc")
					.setSource(jsonBuilder().startObject().field("tweet", tweet).endObject()).get();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
