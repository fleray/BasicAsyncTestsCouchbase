package chatgpt.async;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.MutationResult;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class CouchbaseReactiveUpsertExample {

	private static final String BUCKET_NAME = "myBucket";

	public static void main(String[] args) {
		// Connect to the Couchbase Cluster
		Cluster cluster = Cluster.connect("localhost", "Administrator", "password");
		Bucket bucket = cluster.bucket(BUCKET_NAME);
		bucket.waitUntilReady(Duration.ofSeconds(10));
		ReactiveCollection collection = bucket.defaultCollection().reactive();

		// Generate 10 documents with unique keys and JSON values
		List<JsonObject> documents = IntStream.range(1, 11).mapToObj(
				i -> JsonObject.create().put("id", i).put("name", "User" + i).put("email", "user" + i + "@example.com"))
				.collect(Collectors.toList());

		// Upsert all documents asynchronously
		Flux.fromIterable(documents).flatMap(doc -> {
			String documentId = "doc-" + doc.getInt("id"); // Generate a unique document ID
			return createOrUpdateDocument(collection, doc, documentId);
		}).blockLast();
		
//			.then() // Wait for all upserts to complete
//				.doFinally(signal -> cluster.disconnect()) // Disconnect from the cluster
//				.subscribe(unused -> {
//				}, error -> System.err.println("Error: " + error.getMessage()),
//						() -> System.out.println("All documents upserted successfully!"));
	}

	/**
	 * Créer ou mettre à jour un document
	 */

	public static Mono<Void> createOrUpdateDocument(ReactiveCollection collection, JsonObject doc,
			String documentId) {
		return collection.upsert(documentId, doc).doOnSuccess(result -> System.out.println("Upserted: " + documentId))
				.doOnError(e -> System.err.println("Error upserting " + documentId + ": " + e.getMessage()))
				.then();
	}

}
