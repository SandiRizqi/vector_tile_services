// src/db/migrations.rs
use sqlx::PgPool;

pub async fn run_migrations(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        CREATE OR REPLACE FUNCTION public.get_tile(
            p_table text,
            p_geom_col text,
            p_z integer,
            p_x integer,
            p_y integer,
            p_minx double precision,
            p_miny double precision,
            p_maxx double precision,
            p_maxy double precision
        )
        RETURNS bytea AS
        $$
        DECLARE
            mvt bytea;
            bbox geometry;
            sql_query text;
        BEGIN
            -- Pakai ST_MakeEnvelope seperti query lama
            bbox := ST_MakeEnvelope(p_minx, p_miny, p_maxx, p_maxy, 3857);
            
            sql_query := format(
                $f$
                SELECT ST_AsMVT(q, %L, 4096, 'geom') FROM (
                    SELECT
                        id,
                        ST_AsMVTGeom(
                            ST_Transform(
                                CASE
                                    WHEN %s >= 17 THEN %I
                                    WHEN %s <= 5 THEN ST_SimplifyVW(%I, %s)
                                    WHEN %s <= 8 THEN ST_SimplifyVW(%I, %s)
                                    ELSE ST_SimplifyVW(%I, %s)
                                END,
                                3857
                            ),
                            $1,
                            4096, 256, true
                        ) AS geom
                    FROM %I
                    WHERE %I && ST_Transform($1, ST_SRID(%I))
                ) AS q
                WHERE q.geom IS NOT NULL
                $f$,
                p_table,
                p_z, p_geom_col,
                p_z, p_geom_col, 1e-6 * POWER(2, 17 - p_z),
                p_z, p_geom_col, 1e-7 * POWER(2, 17 - p_z),
                p_geom_col, 1e-8 * POWER(2, 17 - p_z),
                p_table,
                p_geom_col,
                p_geom_col
            );
            
            EXECUTE sql_query INTO mvt USING bbox;
            
            RETURN COALESCE(mvt, ''::bytea);
            END;
            $$ LANGUAGE plpgsql STABLE STRICT;
        "#
    )
    .execute(pool)
    .await?;

    println!("✓ Function get_tile created/updated successfully");
    
    Ok(())
}