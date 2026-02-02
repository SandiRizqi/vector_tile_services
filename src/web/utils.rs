use std::f64::consts::PI;
use sqlx::PgPool;
use sqlx::Row;


pub struct BBox {
   pub minx: f64,
   pub miny: f64,
   pub maxx: f64,
   pub maxy: f64,
}


pub fn tile_to_bbox(z: u32, x: u32, y: u32) -> BBox {
    let n = 2u32.pow(z) as f64;
    
    // Longitude dalam derajat
    let lon_deg_min = x as f64 / n * 360.0 - 180.0;
    let lon_deg_max = (x + 1) as f64 / n * 360.0 - 180.0;
    
    // Latitude dalam radian (inverse Web Mercator tile formula)
    let lat_rad_min = ((PI * (1.0 - 2.0 * (y + 1) as f64 / n)).sinh()).atan();
    let lat_rad_max = ((PI * (1.0 - 2.0 * y as f64 / n)).sinh()).atan();
    
    // Konversi ke Web Mercator (EPSG:3857)
    const R: f64 = 6378137.0;
    
    let minx = R * lon_deg_min.to_radians();
    let maxx = R * lon_deg_max.to_radians();
    let miny = R * ((PI / 4.0 + lat_rad_min / 2.0).tan().ln());
    let maxy = R * ((PI / 4.0 + lat_rad_max / 2.0).tan().ln());
    
    BBox { minx, miny, maxx, maxy }
}


/// Check geom column dan GiST index, buat index jika belum ada
pub async fn check_and_create_geom_index(db_pool: &PgPool) -> Result<(), sqlx::Error> {
    println!("\n📊 Checking geom column status...\n");

    // Ambil semua table dari geometry_columns
    let rows = sqlx::query(
        r#"
        SELECT 
            gc.f_table_schema,
            gc.f_table_name,
            gc.f_geometry_column AS geom_col,
            i.indexname as index_name
        FROM public.geometry_columns gc
        LEFT JOIN pg_indexes i
            ON gc.f_table_schema = i.schemaname
            AND gc.f_table_name = i.tablename
            AND i.indexname LIKE 'idx_%_geom_gist'
        ORDER BY gc.f_table_schema, gc.f_table_name
        "#
    )
    .fetch_all(db_pool)
    .await?;

    println!("{:<30} {:<15} {:<10}", "Table", "Geom Column", "Index");
    println!("{}", "─".repeat(60));

    for row in rows {
        let schema: String = row.try_get("f_table_schema")?;
        let table: String = row.try_get("f_table_name")?;
        let geom_col: String = row.try_get("geom_col")?;
        let index_name: Option<String> = row.try_get("index_name")?;

        let has_idx = if index_name.is_some() { "YES" } else { "NO" };

        let table_name = format!("{}.{}", schema, table);
        println!("{:<30} {:<15} {:<10}", table_name, geom_col, has_idx);

        // 🔹 Jika index belum ada → buat GiST index
        if index_name.is_none() {
            let idx_name = format!("idx_{}_geom_gist", table);
            let sql = format!(
                "CREATE INDEX CONCURRENTLY {} ON {}.{} USING GIST({})",
                idx_name, schema, table, geom_col
            );
            sqlx::query(&sql).execute(db_pool).await?;
            println!("✅ Created GiST index: {}.{}", schema, idx_name);
        }
    }

    println!();
    Ok(())
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
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS {} ON {}.{} USING GIST({})",
            index_name, schema, table, geom_col
        );
        sqlx::query(&create_index_sql)
            .execute(db_pool)
            .await?;
        println!("Created GiST index: {}.{}", schema, index_name);
    }

    Ok(())
}
