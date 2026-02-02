// src/db/migrations.rs
use sqlx::PgPool;

pub async fn run_migrations(pool: &PgPool) -> Result<(), sqlx::Error> {
    // Create or replace the get_tile function
    sqlx::query(
        r#"
        CREATE OR REPLACE FUNCTION public.get_tile(
            p_table text,
            p_geom_col text,
            p_z integer,
            p_x integer,
            p_y integer
        )
        RETURNS bytea AS
        $$
        DECLARE
            mvt bytea;
            bbox geometry;
        BEGIN
            bbox := ST_TileEnvelope(p_z, p_x, p_y);
            EXECUTE format(
                $f$
                SELECT ST_AsMVT(q, '%I', 4096, 'geom') FROM (
                    SELECT
                        ST_AsMVTGeom(
                            ST_Transform(
                                CASE
                                    WHEN %L >= 17 THEN %I
                                    WHEN %L <= 5 THEN ST_SimplifyVW(%I, 1e-6 * POWER(2, 17 - %L))
                                    WHEN %L <= 8 THEN ST_SimplifyVW(%I, 1e-7 * POWER(2, 17 - %L))
                                    ELSE ST_SimplifyVW(%I, 1e-8 * POWER(2, 17 - %L))
                                END,
                                3857
                            ),
                            %L,
                            4096, 256, true
                        ) AS geom
                    FROM %I
                    WHERE %I && ST_Transform(%L, ST_SRID(%I))
                ) AS q
                WHERE q.geom IS NOT NULL
                $f$,
                p_table, p_z, p_geom_col, p_z, p_geom_col, p_z, 
                p_geom_col, p_z, p_geom_col, bbox, p_table, 
                p_geom_col, bbox, p_geom_col
            ) INTO mvt;
            RETURN mvt;
        END;
        $$ LANGUAGE plpgsql STABLE STRICT;
        "#
    )
    .execute(pool)
    .await?;

    println!("✓ Function get_tile created/updated successfully");
    
    Ok(())
}