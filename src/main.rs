
use actix_web::{web, App, HttpServer};
use actix_web::middleware::Logger;
use sqlx::postgres::PgPoolOptions;
use log::{info, error};
use dotenv::dotenv;
use vector_tile_services::web::{web_handler, utils, db};
use std::time::Duration;


#[actix_web::main]
async fn main() -> std::io::Result<()> {

    dotenv().ok();

    let db_url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set in .env");

    let pool = PgPoolOptions::new()
    .max_connections(16)
    .min_connections(4)            // keep-alive connections
    .acquire_timeout(Duration::from_secs(3))
    .idle_timeout(Duration::from_secs(600))
    .max_lifetime(Duration::from_secs(1800))       // set max pool size
    .connect(&db_url)
    .await
    .expect("Failed to connect to database");


    db::run_migrations(&pool)
        .await
        .expect("Failed to run migrations");




    let port: u16 = std::env::var("PORT")
    .unwrap_or_else(|_| "8080".to_string())
    .parse()
    .expect("PORT must be a number");

    let host = "0.0.0.0";

    env_logger::init();
    info!("Loading layers... .");
    


    // match web_handler::load_layers(&pool).await {
    //     Ok(layers) => {
    //         let mut cache = web_handler::LAYERS_CACHE.write().await;
    //         *cache = Some(layers);
    //         info!("Layers cache loaded at startup!");
    //     }
    //     Err(e) => error!("Failed to load layers cache: {:?}", e),
    // };

    match utils::check_and_create_geom_index(&pool).await {
        Ok(()) => {
            info!("Checking geom success!");
        }
        Err(e) => error!("Failed to check geom status: {:?}", e),
    };

    


    

    info!("starting server at port {}", port);
    HttpServer::new( move || {
        App::new()
            .wrap(Logger::default())
            .app_data(web::Data::new(pool.clone()))
            .route("/", web::get().to(web_handler::index))
            .route("/layers", web::get().to(web_handler::get_layers))
            .route("/layer_list", web::get().to(web_handler::layer_list))
            .route("/tiles/{table_name}/{z}/{x}/{y}.pbf", web::get().to(web_handler::get_vector_tile))
            .default_service(web::route().to(web_handler::not_found))
    })
    .bind((host, port))?
    .run()
    .await
}

