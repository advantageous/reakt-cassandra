package io.advantageous.reakt.cassandra;

import com.datastax.driver.core.*;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.advantageous.reakt.promise.Promise;
import io.advantageous.reakt.promise.Promises;

/** Thin wrapper around Cassandra session to expose Cassandra's async methods as Reakt promises. */
public class AsyncSession {


    private final Session session;

    public AsyncSession(Session session) {
        this.session = session;
    }

    public static AsyncSession asyncSession(final Session session) {
        return new AsyncSession(session);
    }

    /**
     * @param future guava future
     * @param <T>    type of future
     * @return Reakt promise
     */
    public static <T> Promise<T> futureToPromise(final ListenableFuture<T> future) {
        return Promises.invokablePromise(promise ->
                Futures.addCallback(future, new FutureCallback<T>() {
                    public void onSuccess(T result) {
                        promise.resolve(result);
                    }
                    public void onFailure(Throwable thrown) {
                        promise.reject(thrown);
                    }
                }));
    }

    /**
     * Gets the underlying session
     * @return cassandra session
     */
    public Session getSession() {
        return session;
    }

    /**
     * Closes the session
     *
     * @return promise
     */
    public Promise<Void> close() {
        return futureToPromise(session.closeAsync());
    }

    /**
     * Prepares the  query string asynchronously.
     *
     * @param query query to prepare
     * @return promise of PreparedStatement.
     */
    public Promise<PreparedStatement> prepare(final String query) {
        return futureToPromise(session.prepareAsync(query));
    }

    /**
     * Prepares the  query string asynchronously.
     *
     * @param query query to prepare
     * @return promise of PreparedStatement.
     */
    public Promise<PreparedStatement> prepare(final RegularStatement query) {
        return futureToPromise(session.prepareAsync(query));
    }


    /**
     * Executes query asynchronously.
     *
     * @param statement - the CQL query to execute (that can be either any Statement.
     * @return result promise
     */
    public Promise<ResultSet> execute(final Statement statement) {
        return futureToPromise(session.executeAsync(statement));
    }
}
