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


// /// Check geom column dan GiST index, buat index jika belum ada
// pub async fn check_and_create_geom_index(db_pool: &PgPool) -> Result<(), sqlx::Error> {
//     println!("\n📊 Checking geom column status...\n");

//     // Ambil semua table dari geometry_columns
//     let rows = sqlx::query(
//         r#"
//         SELECT 
//             gc.f_table_schema,
//             gc.f_table_name,
//             gc.f_geometry_column AS geom_col,
//             i.indexname as index_name
//         FROM public.geometry_columns gc
//         JOIN pg_class c
//             ON c.relname = gc.f_table_name
//         JOIN pg_namespace n
//             ON n.oid = c.relnamespace
//             AND n.nspname = gc.f_table_schema
//         LEFT JOIN pg_indexes i
//             ON gc.f_table_schema = i.schemaname
//             AND gc.f_table_name = i.tablename
//             AND i.indexname LIKE 'idx_%_geom_gist'
//         WHERE c.relkind IN ('r','m')
//         ORDER BY gc.f_table_schema, gc.f_table_name
//         "#
//     )
//     .fetch_all(db_pool)
//     .await?;

//     println!("{:<30} {:<15} {:<10}", "Table", "Geom Column", "Index");
//     println!("{}", "─".repeat(60));

//     for row in rows {
//         let schema: String = row.try_get("f_table_schema")?;
//         let table: String = row.try_get("f_table_name")?;
//         let geom_col: String = row.try_get("geom_col")?;
//         let index_name: Option<String> = row.try_get("index_name")?;

//         let has_idx = if index_name.is_some() { "YES" } else { "NO" };

//         let table_name = format!("{}.{}", schema, table);
//         println!("{:<30} {:<15} {:<10}", table_name, geom_col, has_idx);

//         // 🔹 Jika index belum ada → buat GiST index
//         if index_name.is_none() {
//             let idx_name = format!("idx_{}_geom_gist", table);
//             let sql = format!(
//                 "CREATE INDEX CONCURRENTLY {} ON {}.{} USING GIST({})",
//                 idx_name, schema, table, geom_col
//             );
//             sqlx::query(&sql).execute(db_pool).await?;
//             println!("✅ Created GiST index: {}.{}", schema, idx_name);
//         }
//     }

//     println!();
//     Ok(())
// }


/// Check geom column dan GiST index, buat kolom geom_3857 dan index jika belum ada
pub async fn check_and_create_geom_index(db_pool: &PgPool) -> Result<(), sqlx::Error> {
    println!("\n📊 Checking geom column status...\n");

    // Ambil semua table dari geometry_columns
    let rows = sqlx::query(
        r#"
        SELECT 
            gc.f_table_schema,
            gc.f_table_name,
            gc.f_geometry_column AS geom_col,
            gc.type AS geom_type,
            gc.srid
        FROM public.geometry_columns gc
        JOIN pg_class c
            ON c.relname = gc.f_table_name
        JOIN pg_namespace n
            ON n.oid = c.relnamespace
            AND n.nspname = gc.f_table_schema
        WHERE c.relkind IN ('r','m')
        ORDER BY gc.f_table_schema, gc.f_table_name
        "#
    )
    .fetch_all(db_pool)
    .await?;

    println!("{:<30} {:<15} {:<15} {:<15} {:<10}", "Table", "Geom Column", "Geom Type", "geom_3857", "Index");
    println!("{}", "─".repeat(90));

    for row in rows {
        let schema: String = row.try_get("f_table_schema")?;
        let table: String = row.try_get("f_table_name")?;
        let geom_col: String = row.try_get("geom_col")?;
        let geom_type: String = row.try_get("geom_type")?;
        let srid: i32 = row.try_get("srid")?;

        let table_name = format!("{}.{}", schema, table);

        // 🔹 Cek apakah kolom geom_3857 sudah ada
        let check_col = sqlx::query(
            r#"
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = $1 
            AND table_name = $2 
            AND column_name = 'geom_3857'
            "#
        )
        .bind(&schema)
        .bind(&table)
        .fetch_optional(db_pool)
        .await?;

        let has_geom_3857 = check_col.is_some();
        let geom_3857_status = if has_geom_3857 { "YES" } else { "NO" };

        // 🔹 Jika kolom geom_3857 belum ada → buat kolom baru dan transform dari kolom asli
        if !has_geom_3857 {
            println!("{:<30} {:<15} {:<15} {:<15} {:<10}", table_name, geom_col, geom_type, "CREATING...", "-");
            
            // Tambah kolom geom_3857 dengan tipe geometri 2D saja
            let add_col_sql = format!(
                "ALTER TABLE {}.{} ADD COLUMN geom_3857 geometry({}, 3857)",
                schema, table, geom_type
            );
            sqlx::query(&add_col_sql).execute(db_pool).await?;
            println!("   ✅ Created column: geom_3857 ({}, 3857)", geom_type);

            
        }

        // Transform dan isi data dari kolom asli ke geom_3857 (force 2D dengan ST_Force2D)
        let transform_sql = format!(
                "UPDATE {}.{} SET geom_3857 = ST_Transform(ST_Force2D({}), 3857) WHERE geom_3857 IS NULL",
                schema, table, geom_col
            );
        sqlx::query(&transform_sql).execute(db_pool).await?;
            println!("   ✅ Transformed data from SRID {} to 3857 (forced to 2D)", srid);

        // 🔹 Cek apakah index sudah ada
        let idx_name = format!("idx_{}_geom_3857_gist", table);
        let check_idx = sqlx::query(
            r#"
            SELECT indexname 
            FROM pg_indexes 
            WHERE schemaname = $1 
            AND tablename = $2 
            AND indexname = $3
            "#
        )
        .bind(&schema)
        .bind(&table)
        .bind(&idx_name)
        .fetch_optional(db_pool)
        .await?;

        let has_index = check_idx.is_some();
        let index_status = if has_index { "YES" } else { "NO" };

        if has_geom_3857 {
            println!("{:<30} {:<15} {:<15} {:<15} {:<10}", table_name, geom_col, geom_type, geom_3857_status, index_status);
        }

        // 🔹 Jika index belum ada → buat GiST index pada geom_3857
        if !has_index {
            let sql = format!(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS {} ON {}.{} USING GIST(geom_3857)",
                idx_name, schema, table
            );
            sqlx::query(&sql).execute(db_pool).await?;
            println!("   ✅ Created GiST index: {}.{}", schema, idx_name);
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
