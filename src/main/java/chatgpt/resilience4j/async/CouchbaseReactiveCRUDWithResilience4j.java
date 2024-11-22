package chatgpt.resilience4j.async;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.reactor.retry.RetryOperator;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * This class sample is NOT working as of today.
 * @author fabriceleray
 *
 */
public class CouchbaseReactiveCRUDWithResilience4j {

    private static final String BUCKET_NAME = "myBucket";
    private static final String DOCUMENT_ID = "doc123";

    public static void main(String[] args) {
        // Connexion au cluster Couchbase
        Cluster cluster = Cluster.connect("localhost", "username", "password");
        ReactiveCollection collection = cluster.bucket(BUCKET_NAME).defaultCollection().reactive();

        // Configuration de Resilience4j Retry
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3) // Nombre maximal de tentatives
                .waitDuration(Duration.ofSeconds(2)) // Temps d'attente entre les tentatives
                .retryExceptions(RuntimeException.class) // Exceptions à gérer
                .build();
        Retry retry = Retry.of("couchbaseRetry", retryConfig);
        RetryOperator<Object> retryOperator = RetryOperator.of(retry);

        // Exécution des opérations CRUD
        performOperationWithRetry(createOrUpdateDocument(collection), retryOperator)
                .then(performOperationWithRetry(readDocument(collection), retryOperator))
                .then(performOperationWithRetry(deleteDocument(collection), retryOperator))
                .doOnError(e -> System.err.println("Final failure: " + e.getMessage()))
                .doFinally(signal -> cluster.disconnect())
                .subscribe();
    }

    /**
     * Créer ou mettre à jour un document
     */
    private static Mono<Void> createOrUpdateDocument(ReactiveCollection collection) {
        JsonObject content = JsonObject.create()
                .put("name", "John Doe")
                .put("email", "john.doe@example.com");

        return collection.upsert(DOCUMENT_ID, content)
                .doOnSuccess(result -> System.out.println("Document created/updated: " + result))
                .doOnError(e -> System.err.println("Error during Create/Update: " + e.getMessage()))
                .then(); // Convertir en Mono<Void>
    }

    /**
     * Lire un document depuis le nœud principal ou une réplique
     */
    private static Mono<Void> readDocument(ReactiveCollection collection) {
        return collection.get(DOCUMENT_ID)
                .doOnSuccess(result ->
                        System.out.println("Document read from primary: " + result.contentAsObject()))
                .onErrorResume(primaryError -> {
                    System.err.println("Primary read failed, attempting replica: " + primaryError.getMessage());
                    return collection.getAnyReplica(DOCUMENT_ID)
                            .doOnSuccess(replicaResult ->
                                    System.out.println("Document read from replica: " + replicaResult.contentAsObject()))
                            .doOnError(replicaError ->
                                    System.err.println("Replica read also failed: " + replicaError.getMessage()));
                })
                .then(); // Convertir en Mono<Void>
    }

    /**
     * Supprimer un document
     */
    private static Mono<Void> deleteDocument(ReactiveCollection collection) {
        return collection.remove(DOCUMENT_ID)
                .doOnSuccess(result -> System.out.println("Document deleted: " + result))
                .doOnError(e -> System.err.println("Error during Delete: " + e.getMessage()))
                .then(); // Convertir en Mono<Void>
    }

    /**
     * Envelopper une opération avec Resilience4j Retry
     */
    private static Mono<Void> performOperationWithRetry(Mono<Void> mono, RetryOperator<Object> retryOperator) {
        //return mono.transform(retryOperator);
    	// TODO: Fix issue here
    	return null;
    }
}

