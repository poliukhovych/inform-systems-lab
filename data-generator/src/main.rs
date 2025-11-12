use actix_web::{get, App, HttpResponse, HttpServer, Responder};
use anyhow::Result;
use lazy_static::lazy_static;
use lapin::{
    options::{BasicPublishOptions, QueueDeclareOptions},
    types::FieldTable,
    BasicProperties, Channel, Connection, ConnectionProperties,
};
use prometheus::{
    register_histogram_vec, register_int_counter_vec, Encoder, HistogramVec, IntCounterVec,
    TextEncoder,
};
use rand::Rng;
use std::{env, time::Duration};
use tokio::time::sleep;
use tokio_postgres::{Client, NoTls};

lazy_static! {
    static ref USER_ACTIONS_TOTAL: IntCounterVec = register_int_counter_vec!(
        "user_actions_total",
        "Total number of user actions.",
        &["action"]
    )
    .unwrap();

    static ref DB_QUERY_LATENCY_SECONDS: HistogramVec = register_histogram_vec!(
        "db_query_latency_seconds",
        "Latency of database queries in seconds.",
        &["query_type"]
    )
    .unwrap();
}

async fn connect_postgres() -> Result<Client> {
    let conn_str = env::var("PG_CONN")
        .unwrap_or_else(|_| "host=db user=user password=password dbname=metrics_db".into());
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("postgres connection error: {e}");
        }
    });
    Ok(client)
}

async fn connect_rabbitmq() -> Result<Channel> {
    let addr = env::var("AMQP_ADDR")
        .unwrap_or_else(|_| "amqp://admin:admin@rabbitmq:5672/%2f".into());
    let conn = Connection::connect(&addr, ConnectionProperties::default()).await?;
    let ch = conn.create_channel().await?;
    ch.queue_declare(
        "demo.queue",
        QueueDeclareOptions::default(),
        FieldTable::default(),
    )
        .await?;
    Ok(ch)
}

async fn insert_user_action(client: &Client, action: &str, user_id: i32) -> Result<()> {
    use std::time::Instant;
    let start = Instant::now();
    let query_type = "insert_user_action";

    client
        .execute(
            "INSERT INTO user_activity (user_id, action) VALUES ($1, $2)",
            &[&user_id, &action],
        )
        .await?;

    let duration = start.elapsed().as_secs_f64();
    USER_ACTIONS_TOTAL.with_label_values(&[action]).inc();
    DB_QUERY_LATENCY_SECONDS
        .with_label_values(&[query_type])
        .observe(duration);

    Ok(())
}

async fn publish_action(ch: &Channel, action: &str, user_id: i32) -> Result<()> {
    let payload = format!(r#"{{"user_id": {user_id}, "action": "{action}"}}"#);
    ch.basic_publish(
        "",
        "demo.queue",
        BasicPublishOptions::default(),
        payload.as_bytes(),
        BasicProperties::default(),
    )
        .await?
        .await?;
    Ok(())
}

async fn run_simulation(pg: Client, ch: Channel) {
    let actions = ["login", "view_profile", "add_item", "logout"];

    loop {
        let (action, user_id) = {
            let mut rng = rand::thread_rng();
            let a = actions[rng.gen_range(0..actions.len())];
            let u = rng.gen_range(1..100);
            (a, u)
        };

        if let Err(e) = insert_user_action(&pg, action, user_id).await {
            eprintln!("DB insert failed: {e}");
        }
        if let Err(e) = publish_action(&ch, action, user_id).await {
            eprintln!("AMQP publish failed: {e}");
        }

        let ms = {
            let mut rng = rand::thread_rng();
            rng.gen_range(500..2000)
        };
        sleep(Duration::from_millis(ms)).await;
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

    let pg = connect_postgres().await.expect("postgres connect failed");
    let ch = connect_rabbitmq().await.expect("rabbitmq connect failed");

    tokio::spawn(run_simulation(pg, ch));

    HttpServer::new(|| App::new().service(metrics_handler))
        .bind(("0.0.0.0", 9091))?
        .run()
        .await
}
