use actix_web::{web, HttpResponse, Responder};
use sqlx::PgPool;
use sqlx::Row;
use std::fs;
use serde::{Serialize, Deserialize};
use once_cell::sync::Lazy;
use tokio::sync::RwLock;
use log::{error};
use super::utils; 


#[derive(Serialize, Clone)]
pub struct Layer {
    table_name: String,
    geom_column: String,
    geom_type: String,
    srid: i32,
    bbox: [f64; 4], // [minx, miny, maxx, maxy]
    url: String,
}

impl Layer {
    fn new(table_name: String, geom_column: String, geom_type: String, srid: i32, bbox: [f64; 4],) -> Self {
        let mut layer = Self {
                table_name: table_name.to_string(), 
                geom_column: geom_column.to_string(), 
                geom_type: geom_type.to_string(), 
                srid: srid, 
                bbox: bbox,
                url: String::new(), 
            };

        layer.url = layer.generate_url();
        layer

    }

    fn generate_url (&self) -> String {
        format!("http://127.0.0.1:8080/tiles/{}/{{z}}/{{x}}/{{y}}.pbf", self.table_name)
    }
    
}


#[derive(Deserialize)]
pub struct TilePath {
    table_name: String,
    z: u32,
    x: u32,
    y: u32,
}




pub static LAYERS_CACHE: Lazy<RwLock<Option<Vec<Layer>>>> =
    Lazy::new(|| RwLock::new(None));



pub async fn index() -> HttpResponse {
    match fs::read_to_string("static/index.html") {
        Ok(html) => HttpResponse::Ok()
            .content_type("text/html; charset=utf-8")
            .body(html),
        Err(_) => HttpResponse::InternalServerError()
            .body("Failed to load HTML"),
    }
}


pub async fn not_found() -> HttpResponse {
    match fs::read_to_string("static/404.html") {
        Ok(html) => HttpResponse::Ok()
            .content_type("text/html; charset=utf-8")
            .body(html),
        Err(_) => HttpResponse::InternalServerError()
            .body("Failed to load HTML"),
    }
}


pub async fn layer_list() -> HttpResponse {
    match fs::read_to_string("static/layer_list.html") {
        Ok(html) => HttpResponse::Ok()
            .content_type("text/html; charset=utf-8")
            .body(html),
        Err(_) => HttpResponse::InternalServerError()
            .body("Failed to load HTML"),
    }
}





pub async fn get_layers (db_pool: web::Data<PgPool>) -> impl Responder {

     {
        let cache = LAYERS_CACHE.read().await;
        if let Some(layers) = &*cache {
            return HttpResponse::Ok().json(layers);
        }
    }

    let pool: &PgPool = db_pool.get_ref();

     let rows= sqlx::query(
       r#"
        SELECT f_table_schema, f_table_name, f_geometry_column, type, srid
        FROM public.geometry_columns
        "#
    )
    .fetch_all(pool)
    .await;

    let rows = match rows {
        Ok(t) => t,
        Err(e) => {
            eprintln!("DB error: {:?}", e);
            return HttpResponse::InternalServerError().body("Failed to fetch tables");
        }
    };

    let mut layers: Vec<Layer> = Vec::new();

    for t in rows {
        let schema: String = t.try_get("f_table_schema").unwrap();
        let table: String = t.try_get("f_table_name").unwrap();
        let geom_col: String = t.try_get("f_geometry_column").unwrap();
        let geom_type: String = t.try_get("type").unwrap();
        let srid: i32 = t.try_get("srid").unwrap();

        // Dynamic ST_Extent
        let sql = format!(
            "SELECT \
                ST_XMin(ST_Extent({geom})) AS minx, \
                ST_YMin(ST_Extent({geom})) AS miny, \
                ST_XMax(ST_Extent({geom})) AS maxx, \
                ST_YMax(ST_Extent({geom})) AS maxy \
             FROM {}.{}",
            schema, table,
            geom = geom_col
        );

        let row = sqlx::query(&sql)
            .fetch_one(pool)
            .await
            .expect("Failed to get extent");

        let bbox = [
            row.try_get::<f64, _>("minx").unwrap_or(0.0),
            row.try_get::<f64, _>("miny").unwrap_or(0.0),
            row.try_get::<f64, _>("maxx").unwrap_or(0.0),
            row.try_get::<f64, _>("maxy").unwrap_or(0.0),
        ];

        layers.push(Layer::new(table, geom_col, geom_type, srid, bbox));
    }


    {
        let mut cache = LAYERS_CACHE.write().await;
        *cache = Some(layers.clone());
    }

    HttpResponse::Ok().json(layers)
}



pub async fn create_index(
    db_pool: &PgPool,
    schema: &str,
    table: &str,
    geom_col: &str,
) -> Result<(), sqlx::Error> {
    // 1️⃣ Cek apakah index sudah ada
    let index_check_sql = r#"
        SELECT indexname 
        FROM pg_indexes 
        WHERE schemaname = $1 AND tablename = $2 AND indexname = $3
    "#;

    // Nama index: idx_{table}_{geom}_gist
    let index_name = format!("idx_{}_{}_gist", table, geom_col);

    let index_exists: Option<(String,)> = sqlx::query_as(index_check_sql)
        .bind(schema)
        .bind(table)
        .bind(&index_name)
        .fetch_optional(db_pool)
        .await?;

    // 2️⃣ Buat index kalau belum ada
    if index_exists.is_none() {
        let create_index_sql = format!(
            "CREATE INDEX IF NOT EXISTS {} ON {}.{} USING GIST({})",
            index_name, schema, table, geom_col
        );
        sqlx::query(&create_index_sql)
            .execute(db_pool)
            .await?;
        println!("Created GiST index: {}.{}", schema, index_name);
    }

    Ok(())
}


pub async fn load_layers(db_pool: &PgPool) -> Result<Vec<Layer>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        SELECT f_table_schema, f_table_name, f_geometry_column, type, srid
        FROM public.geometry_columns
        "#
    )
    .fetch_all(db_pool)
    .await?;

    let mut layers: Vec<Layer> = Vec::new();

    for t in rows {
        let schema: String = t.try_get("f_table_schema")?;
        let table: String = t.try_get("f_table_name")?;
        let geom_col: String = t.try_get("f_geometry_column")?;
        let geom_type: String = t.try_get("type")?;
        let srid: i32 = t.try_get("srid")?;


        // Create Index if not Exist
        create_index(db_pool, &schema, &table, &geom_col).await?;

        


        // Generate Min Max Bounds

        let sql = format!(
            "SELECT \
                ST_XMin(ST_Extent({geom})) AS minx, \
                ST_YMin(ST_Extent({geom})) AS miny, \
                ST_XMax(ST_Extent({geom})) AS maxx, \
                ST_YMax(ST_Extent({geom})) AS maxy \
             FROM {}.{}",
            schema, table,
            geom = geom_col
        );

        let row = sqlx::query(&sql)
            .fetch_one(db_pool)
            .await?;

        let bbox = [
            row.try_get::<f64, _>("minx").unwrap_or(0.0),
            row.try_get::<f64, _>("miny").unwrap_or(0.0),
            row.try_get::<f64, _>("maxx").unwrap_or(0.0),
            row.try_get::<f64, _>("maxy").unwrap_or(0.0),
        ];

        layers.push(Layer::new(table, geom_col, geom_type, srid, bbox));
    }

    Ok(layers)
}


async fn get_layer_detail(table_name: String) -> Option<Layer> {
    // 1️⃣ Ambil guard terlebih dahulu
    let cache_guard = LAYERS_CACHE.read().await;
    // 2️⃣ Ambil reference ke Vec<Layer>
    let layers = match cache_guard.as_ref() {
        Some(l) => l,
        None => {
            error!("Cache not loaded");
            return None;
        }
    };
    // 3️⃣ Cari layer
    let layer = match layers.iter().find(|l| l.table_name == table_name) {
        Some(l) => l.clone(), // perlu clone karena kita return owned Layer
        None => {
            error!("Layer not found: {}", table_name);
            return None;
        }
    };

    Some(layer)
}


pub async fn get_vector_tile(
    db_pool: web::Data<PgPool>,
    path: web::Path<TilePath>,
) -> impl  Responder {
    let params = path.into_inner();
    let layer = match get_layer_detail(params.table_name).await {
        Some(l) => l,
        None => return HttpResponse::NotFound().body("Layer not found"),
    };

    // 2️⃣ Hitung bounding box tile Web Mercator
    let tile_bbox = utils::tile_to_bbox(params.z, params.x, params.y);

    // 3️⃣ Buat SQL ST_AsMVT
    let sql = format!(
        r#"
        WITH mvtgeom AS (
            SELECT ST_AsMVTGeom(
                ST_Transform({geom_col}, 3857),
                ST_MakeEnvelope({minx}, {miny}, {maxx}, {maxy}, 3857)
            ) AS geom
            FROM {table}
        )
        SELECT ST_AsMVT(mvtgeom.*, '{table}', 4096, 'geom') AS tile
        FROM mvtgeom
        "#,
        geom_col = layer.geom_column,
        table = layer.table_name,
        minx = tile_bbox.minx,
        miny = tile_bbox.miny,
        maxx = tile_bbox.maxx,
        maxy = tile_bbox.maxy
    );

    // 4️⃣ Query ke PostGIS
    let row = sqlx::query(&sql).fetch_one(db_pool.get_ref()).await;

    match row {
        Ok(r) => {
            let tile: Vec<u8> = r.try_get("tile").unwrap_or_default();
            if tile.is_empty() {
                HttpResponse::NotFound().body("Tile empty")
            } else {
                HttpResponse::Ok()
                    .content_type("application/x-protobuf")
                    .body(tile)
            }
        }
        Err(e) => {
            error!("Failed to fetch vector tile: {:?}", e);
            HttpResponse::InternalServerError().body("Failed to fetch vector tile")
        }
    }
}




