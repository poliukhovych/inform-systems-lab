use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use serde::{Deserialize, Serialize};
use jsonwebtoken::{encode, Header, EncodingKey};
use chrono::{Utc, Duration};
use rusqlite::{Connection, Result as RusqliteResult};
use std::sync::{Arc, Mutex};
use std::error::Error as StdError;

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    sub: String,
    exp: usize,
}

#[derive(Debug, Deserialize)]
struct AuthInfo {
    username: String,
    password: String,
}

fn initialize_db() -> RusqliteResult<()> {
    let conn = Connection::open("users.db")?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS users (
             id INTEGER PRIMARY KEY,
             username TEXT NOT NULL UNIQUE,
             password TEXT NOT NULL
         )",
        [],
    )?;
    let count: i32 = conn.query_row("SELECT count(*) FROM users WHERE username = 'user1'", [], |row| row.get(0))?;
    if count == 0 {
        conn.execute(
            "INSERT INTO users (username, password) VALUES (?1, ?2)",
            &["user1", "pass1"],
        )?;
    }
    Ok(())
}

fn find_user(conn: Arc<Mutex<Connection>>, username: String) -> RusqliteResult<Option<String>> {
    let conn = conn.lock().unwrap();
    let mut stmt = conn.prepare("SELECT password FROM users WHERE username = ?1")?;
    let mut rows = stmt.query([username])?;

    if let Some(row) = rows.next()? {
        let password: String = row.get(0)?;
        Ok(Some(password))
    } else {
        Ok(None)
    }
}

#[actix_web::post("/login")]
async fn login(info: web::Json<AuthInfo>, db_conn: web::Data<Arc<Mutex<Connection>>>) -> impl Responder {
    let conn_data = db_conn.get_ref().clone();
    let username = info.username.clone();

    let result = web::block(move || find_user(conn_data, username)).await;

    match result {
        Ok(Ok(stored_password)) => {
            if let Some(password) = stored_password {
                if password == info.password {
                    let claims = Claims {
                        sub: info.username.clone(),
                        exp: (Utc::now() + Duration::days(1)).timestamp() as usize,
                    };
                    let token = encode(
                        &Header::default(),
                        &claims,
                        &EncodingKey::from_secret("super_secret_key".as_ref())
                    ).unwrap();
                    return HttpResponse::Ok().json(token);
                }
            }
        },
        Ok(Err(e)) => {
            eprintln!("Database error: {}", e);
            return HttpResponse::InternalServerError().body("Database error");
        },
        Err(e) => {
            eprintln!("Actix-web blocking error: {}", e);
            return HttpResponse::InternalServerError().body("Internal server error");
        }
    }
    HttpResponse::Unauthorized().body("Invalid credentials")
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    println!("Auth Service is starting...");

    if let Err(e) = initialize_db() {
        eprintln!("Failed to initialize database: {}", e);
        return Err(std::io::Error::new(std::io::ErrorKind::Other, "DB init failed"));
    }
    let conn = Arc::new(Mutex::new(Connection::open("users.db").unwrap()));

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(conn.clone()))
            .service(login)
    })
        .bind(("0.0.0.0", 8000))?
        .run()
        .await
}
