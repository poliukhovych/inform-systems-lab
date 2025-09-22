use actix_web::{get, App, HttpServer, Responder, HttpResponse};
use tokio_postgres::{NoTls, Error};
use std::time::Instant;
use prometheus::{Encoder, TextEncoder, IntCounterVec, HistogramVec, register_int_counter_vec, register_histogram_vec};
use lazy_static::lazy_static;
use rand::Rng;

lazy_static! {
    static ref USER_ACTIONS_TOTAL: IntCounterVec =
        register_int_counter_vec!(
            "user_actions_total",
            "Total number of user actions.",
            &["action"]
        ).unwrap();

    static ref DB_QUERY_LATENCY_SECONDS: HistogramVec =
        register_histogram_vec!(
            "db_query_latency_seconds",
            "Latency of database queries in seconds.",
            &["query_type"]
        ).unwrap();
}

async fn insert_user_action(action: &str, user_id: i32) -> Result<(), Error> {
    let (client, connection) = tokio_postgres::connect(
        "host=db user=user password=password dbname=metrics_db", NoTls
    ).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let start = Instant::now();
    let query_type = "insert_user_action";

    client.execute(
        "INSERT INTO user_activity (user_id, action) VALUES ($1, $2)",
        &[&user_id, &action],
    ).await?;

    let duration = start.elapsed().as_secs_f64();

    USER_ACTIONS_TOTAL.with_label_values(&[action]).inc();
    DB_QUERY_LATENCY_SECONDS.with_label_values(&[query_type]).observe(duration);

    println!("Action '{}' for user {} inserted in {:.4}s.", action, user_id, duration);
    Ok(())
}

fn run_simulation() {
    let mut rng = rand::thread_rng();
    let actions = vec!["login", "view_profile", "add_item", "logout"];

    loop {
        let random_action = actions[rng.gen_range(0..actions.len())];
        let random_user_id = rng.gen_range(1..100);

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            match insert_user_action(random_action, random_user_id).await {
                Ok(_) => (),
                Err(e) => eprintln!("Failed to insert data: {}", e),
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(rng.gen_range(500..2000))).await;
        });
    }
}

#[get("/metrics")]
async fn metrics_handler() -> impl Responder {
    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    encoder.encode(&metric_families, &mut buffer).unwrap();

    HttpResponse::Ok()
        .content_type(encoder.format_type())
        .body(buffer)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    println!("Data Generator Service is starting...");

    tokio::task::spawn_blocking(run_simulation);

    HttpServer::new(|| {
        App::new().service(metrics_handler)
    })
        .bind(("0.0.0.0", 9091))?
        .run()
        .await
}
