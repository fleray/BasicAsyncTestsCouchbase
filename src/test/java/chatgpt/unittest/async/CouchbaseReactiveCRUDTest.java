package chatgpt.unittest.async;

import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.kv.MutationResult;

import chatgpt.async.CouchbaseReactiveCRUD;

import com.couchbase.client.java.json.JsonObject;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

class CouchbaseReactiveCRUDTest {

    @Test
    void testCreateOrUpdateDocument() {
        // Mock ReactiveCollection
        ReactiveCollection mockCollection = Mockito.mock(ReactiveCollection.class);

        // Mock MutationResult to simulate a successful response
        MutationResult mockResult = Mockito.mock(MutationResult.class);
        when(mockCollection.upsert(eq("doc123"), any(JsonObject.class)))
                .thenReturn(Mono.just(mockResult));

        // Call the method under test
        // Mono<Void> resultMono = CouchbaseReactiveCRUD.createOrUpdateDocument(mockCollection);
        Mono<Void> resultMono = createOrUpdateDocument(mockCollection);

        // Verify the behavior using StepVerifier
        StepVerifier.create(resultMono)
                .expectComplete()
                .verify();

        // Verify that the upsert method was called once with the correct parameters
        Mockito.verify(mockCollection).upsert(eq("doc123"), any(JsonObject.class));
    }

    /**
     * The createOrUpdateDocument method under test.
     */
    private Mono<Void> createOrUpdateDocument(ReactiveCollection collection) {
        JsonObject content = JsonObject.create()
                .put("name", "John Doe")
                .put("email", "john.doe@example.com");

        return collection.upsert("doc123", content)
                .doOnSuccess(result -> System.out.println("Document created/updated: " + result))
                .doOnError(e -> System.err.println("Error during Create/Update: " + e.getMessage()))
                .then(); // Convert to Mono<Void>
    }
}
