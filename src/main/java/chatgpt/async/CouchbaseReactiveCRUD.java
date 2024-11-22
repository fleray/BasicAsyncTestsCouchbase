package chatgpt.async;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.MutationResult;
import reactor.core.publisher.Mono;

public class CouchbaseReactiveCRUD {

    private static final String BUCKET_NAME = "myBucket";
    private static final String DOCUMENT_ID = "doc123";

    public static void main(String[] args) {
        // Connexion au cluster Couchbase
        Cluster cluster = Cluster.connect("localhost", "Administrator", "password");
        ReactiveCollection collection = cluster.bucket(BUCKET_NAME).defaultCollection().reactive();

        // Exécution des opérations CRUD
        createOrUpdateDocument(collection)
                .then(readDocument(collection))
                .then(deleteDocument(collection))
                .doOnError(e -> System.err.println("Operation failed: " + e.getMessage()))
                //.doOnTerminate(() -> cluster.disconnect())
                .block();
    }

    /**
     * Créer ou mettre à jour un document
     */
    public static Mono<Void> createOrUpdateDocument(ReactiveCollection collection) {
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
    public static Mono<Void> readDocument(ReactiveCollection collection) {
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
    public static Mono<Void> deleteDocument(ReactiveCollection collection) {
        return collection.remove(DOCUMENT_ID)
                .doOnSuccess(result -> System.out.println("Document deleted: " + result))
                .doOnError(e -> System.err.println("Error during Delete: " + e.getMessage()))
                .then(); // Convertir en Mono<Void>
    }
}
