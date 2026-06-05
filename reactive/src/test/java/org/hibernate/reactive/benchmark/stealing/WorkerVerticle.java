package org.hibernate.reactive.benchmark.stealing;

import org.hibernate.reactive.mutiny.Mutiny;

import io.smallrye.mutiny.Uni;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;

public class WorkerVerticle extends AbstractVerticle {

	static final String ADDRESS_PREFIX = "benchmark.worker.";

	private final Mutiny.SessionFactory sessionFactory;
	private final int index;

	public WorkerVerticle(Mutiny.SessionFactory sessionFactory, int index) {
		this.sessionFactory = sessionFactory;
		this.index = index;
	}

	@Override
	public void start(Promise<Void> startPromise) {
		vertx.eventBus().<JsonObject>consumer( ADDRESS_PREFIX + index, msg -> {
			var body = msg.body();
			int sleepMs = body.getInteger( "sleepMs" );
			sleepThenFind( sleepMs )
					.onSuccess( v -> msg.reply( "ok" ) )
					.onFailure( t -> msg.fail( 500, t.getMessage() ) );
		} );
		startPromise.complete();
	}

	private Future<Void> sleepThenFind(int sleepMs) {
		return Future.fromCompletionStage(
				sessionFactory.withSession( session -> {
					Uni<Object> chain = session.createNativeQuery(
							"SELECT 1 FROM pg_sleep(" + (sleepMs / 1000.0) + ")", Object.class
					).getSingleResult();
//					chain = chain.chain(
//							() -> session.find( World.class, ThreadLocalRandom.current().nextInt( 10_000 ) + 1 )
//					);
					return chain.replaceWithVoid();
				} ).convert().toCompletionStage()
		);
	}
}
