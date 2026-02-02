use actix_web::{web, HttpResponse, Responder, HttpRequest};
use sqlx::PgPool;
use sqlx::Row;
use serde::{Serialize, Deserialize};
use once_cell::sync::Lazy;
use tokio::sync::RwLock;
use log::{error, info};
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
    fn new(table_name: String, geom_column: String, geom_type: String, srid: i32, bbox: [f64; 4],base_url:String) -> Self {
        let mut layer = Self {
                table_name: table_name.to_string(), 
                geom_column: geom_column.to_string(), 
                geom_type: geom_type.to_string(), 
                srid: srid, 
                bbox: bbox,
                url: String::new(), 
            };

        layer.url = layer.generate_url(base_url);
        layer

    }

    fn generate_url (&self, base_url:String) -> String {
        format!("{}/tiles/{}/{{z}}/{{x}}/{{y}}.pbf", base_url, self.table_name)
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



pub async fn index(pool: web::Data<PgPool>, req: HttpRequest) -> HttpResponse {

    match load_layers(&pool, req).await {
        Ok(layers) => {
            let mut cache = LAYERS_CACHE.write().await;
            *cache = Some(layers);
            info!("Layers cache loaded at startup!");
        }
        Err(e) => error!("Failed to load layers cache: {:?}", e),
    };
    HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(include_str!("../../static/index.html"))
}


pub async fn not_found() -> HttpResponse {
     HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(include_str!("../../static/404.html"))
}


pub async fn layer_list() -> HttpResponse {
     HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(include_str!("../../static/layer_list.html"))
}





pub async fn get_layers (db_pool: web::Data<PgPool>, req: HttpRequest) -> impl Responder {

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

        let base_url = {
            let c = req.connection_info();
            format!("{}://{}", c.scheme(), c.host())
        };

        layers.push(Layer::new(table, geom_col, geom_type, srid, bbox, base_url));
    }


    {
        let mut cache = LAYERS_CACHE.write().await;
        *cache = Some(layers.clone());
    }

    HttpResponse::Ok().json(layers)
}




pub async fn load_layers(db_pool: &PgPool, req: HttpRequest) -> Result<Vec<Layer>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        SELECT f_table_schema, f_table_name, f_geometry_column, type, srid
        FROM public.geometry_columns
        "#
    )
    .fetch_all(db_pool)
    .await?;

    let mut layers: Vec<Layer> = Vec::new();
    // utils::cleanup_all_geom_3857(&db_pool).await?;

    for t in rows {
        let schema: String = t.try_get("f_table_schema")?;
        let table: String = t.try_get("f_table_name")?;
        let geom_col: String = t.try_get("f_geometry_column")?;
        let geom_type: String = t.try_get("type")?;
        let srid: i32 = t.try_get("srid")?;


        // Create Index if not Exist
        
        // utils::create_geom_3857_index(db_pool, &schema, &table, &geom_col, &geom_type).await?;
        
        


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

        let base_url = {
            let c = req.connection_info();
            format!("{}://{}", c.scheme(), c.host())
        };

        layers.push(Layer::new(table, geom_col, geom_type, srid, bbox, base_url));
    }
    Ok(layers)
}


// async fn get_layer_detail(table_name: String) -> Option<Layer> {
//     // 1️⃣ Ambil guard terlebih dahulu
//     let cache_guard = LAYERS_CACHE.read().await;
//     // 2️⃣ Ambil reference ke Vec<Layer>
//     let layers = match cache_guard.as_ref() {
//         Some(l) => l,
//         None => {
//             error!("Cache not loaded");
//             return None;
//         }
//     };
//     // 3️⃣ Cari layer
//     let layer = match layers.iter().find(|l| l.table_name == table_name) {
//         Some(l) => l.clone(), // perlu clone karena kita return owned Layer
//         None => {
//             error!("Layer not found: {}", table_name);
//             return None;
//         }
//     };

//     Some(layer)
// }


pub async fn get_layer_detail_from_db(db_pool: &PgPool, table_name: String, base_url: &str) -> Option<Layer> {
    // Query ke database langsung, ambil hanya table_name & geom_column
    let row = match sqlx::query(
        r#"
        SELECT f_table_name, f_geometry_column
        FROM public.geometry_columns
        WHERE f_table_name = $1
        "#
    )
    .bind(table_name)
    .fetch_one(db_pool)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            error!("Failed to query geometry_columns: {}", e);
            return None;
        }
    };

    let table_name: String = row.try_get("f_table_name").ok()?;
    let geom_column: String = row.try_get("f_geometry_column").ok()?;

    // Untuk field lain, bisa pakai default / placeholder
    let layer = Layer::new(
        table_name,
        geom_column,
        "UNKNOWN".to_string(), // placeholder
        0 as i32,                          // placeholder
        [0.0, 0.0, 0.0, 0.0],      // placeholder
        base_url.to_string()
    );

    Some(layer)
}


pub async fn get_vector_tile(
    db_pool: web::Data<PgPool>,
    path: web::Path<TilePath>,
    req: HttpRequest
) -> impl Responder {
    let params = path.into_inner();

    let base_url = {
        let c = req.connection_info();
        format!("{}://{}", c.scheme(), c.host())
    };

    let layer = match get_layer_detail_from_db(&db_pool, params.table_name, &base_url).await {
        Some(l) => l,
        None => return HttpResponse::NotFound().body("Layer not found"),
    };

    // Gunakan fungsi get_tile yang sudah dibuat
    let result = sqlx::query_scalar::<_, Vec<u8>>(
        "SELECT public.get_tile($1, $2, $3, $4, $5)"
    )
    .bind(&layer.table_name)
    .bind(&layer.geom_column)
    .bind(params.z as i32)
    .bind(params.x as i32)
    .bind(params.y as i32)
    .fetch_one(db_pool.get_ref())
    .await;

    match result {
        Ok(tile) => {
            if tile.is_empty() {
                HttpResponse::NoContent().finish()
            } else {
                HttpResponse::Ok()
                    .content_type("application/x-protobuf")
                    .insert_header(("Content-Encoding", "gzip")) // opsional
                    .body(tile)
            }
        }
        Err(e) => {
            error!("Failed to fetch vector tile: {:?}", e);
            HttpResponse::InternalServerError().body("Failed to fetch vector tile")
        }
    }
}