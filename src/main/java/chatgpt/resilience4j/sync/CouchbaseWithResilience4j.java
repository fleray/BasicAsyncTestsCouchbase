package chatgpt.resilience4j.sync;

import com.couchbase.client.java.*;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.MutationResult;

import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;

import java.time.Duration;
import java.util.function.Supplier;

public class CouchbaseWithResilience4j {

    private static final String BUCKET_NAME = "myBucket";
    private static final String DOCUMENT_ID = "doc123";

    public static void main(String[] args) {
        // Couchbase cluster connection
        Cluster cluster = Cluster.connect("localhost", "Administrator", "password");
        Bucket bucket = cluster.bucket(BUCKET_NAME);
        Collection collection = bucket.defaultCollection();
        
        IntervalFunction intervalFn =
        		  IntervalFunction.ofExponentialBackoff(500, 2.0);

        // Retry configuration
        RetryConfig retryConfig = RetryConfig.custom()
        		.maxAttempts(5) // Nombre maximal de tentatives
                .waitDuration(Duration.ofMillis(500)) // Délai initial de 500 ms
                .intervalFunction(intervalFn) // Backoff exponentiel (multiplicateur 2x)
                .retryExceptions(RuntimeException.class) // Exceptions à gérer
                .build();

        Retry retry = Retry.of("couchbaseRetry", retryConfig);

        // Exemple d'opération CRUD
        try {
            // Create or Update
            performOperationWithRetry(() -> createOrUpdateDocument(collection), retry);

            // Read
            performOperationWithRetry(() -> readDocument(collection), retry);

            // Delete
            performOperationWithRetry(() -> deleteDocument(collection), retry);
        } catch (Exception e) {
            System.err.println("Final failure: " + e.getMessage());
        } finally {
            cluster.disconnect();
        }
    }

    private static Void createOrUpdateDocument(Collection collection) {
        try {
            JsonObject content = JsonObject.create()
                    .put("name", "John Doe")
                    .put("email", "john.doe@example.com");

            MutationResult result = collection.upsert(DOCUMENT_ID, content);
            System.out.println("Document created/updated: " + result);
            return null;
        } catch (Exception e) {
            System.err.println("Error during Create/Update: " + e.getMessage());
            throw e;
        }
    }

    private static Void readDocument(Collection collection) {
        try {
            GetResult result = collection.get(DOCUMENT_ID);
            System.out.println("Document read: " + result.contentAsObject());
            return null;
        } catch (Exception e) {
            System.err.println("Error during Read: " + e.getMessage());
            throw e;
        }
    }

    private static Void deleteDocument(Collection collection) {
        try {
            MutationResult result = collection.remove(DOCUMENT_ID);
            System.out.println("Document deleted: " + result);
            return null;
        } catch (Exception e) {
            System.err.println("Error during Delete: " + e.getMessage());
            throw e;
        }
    }

    private static void performOperationWithRetry(Supplier<Void> operation, Retry retry) {
        Retry.decorateSupplier(retry, operation).get();
    }
}
