// src/db/migrations.rs
use sqlx::PgPool;

pub async fn run_migrations(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        CREATE OR REPLACE FUNCTION public.get_tile(
            p_table text,
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
            pk_col text;
            sql_query text;
        BEGIN
            -- 1️⃣ Buat bounding box dalam SRID 3857
            bbox := ST_MakeEnvelope(p_minx, p_miny, p_maxx, p_maxy, 3857);

            -- 2️⃣ Cek kolom PK yang ada: id, gid, atau generate row_number
            SELECT column_name
            INTO pk_col
            FROM information_schema.columns
            WHERE table_name = p_table
            AND column_name IN ('id','gid')
            ORDER BY column_name ASC
            LIMIT 1;

            -- 3️⃣ Buat query dynamic menggunakan geom_3857 (sudah dalam SRID 3857)
            IF pk_col IS NULL THEN
                -- tidak ada id/gid, generate row_number
                sql_query := format($f$
                    SELECT ST_AsMVT(tile, %L, 4096, 'geom', 'gid')
                    FROM (
                        SELECT
                            row_number() OVER () AS gid,
                            ST_AsMVTGeom(
                                CASE
                                    WHEN %s >= 17 THEN geom_3857
                                    WHEN %s <= 5 THEN ST_SimplifyVW(geom_3857, %s)
                                    WHEN %s <= 8 THEN ST_SimplifyVW(geom_3857, %s)
                                    ELSE ST_SimplifyVW(geom_3857, %s)
                                END,
                                $1,
                                4096, 256, true
                            ) AS geom
                        FROM %I
                        WHERE geom_3857 && $1
                    ) tile
                $f$,
                    p_table,
                    p_z,
                    p_z, 1e-6 * POWER(2, 17 - p_z),
                    p_z, 1e-7 * POWER(2, 17 - p_z),
                    1e-8 * POWER(2, 17 - p_z),
                    p_table
                );
            ELSE
                -- pakai kolom PK yang ada
                sql_query := format($f$
                    SELECT ST_AsMVT(tile, %L, 4096, 'geom', 'gid')
                    FROM (
                        SELECT
                            %I AS gid,
                            ST_AsMVTGeom(
                                CASE
                                    WHEN %s >= 17 THEN geom_3857
                                    WHEN %s <= 5 THEN ST_SimplifyVW(geom_3857, %s)
                                    WHEN %s <= 8 THEN ST_SimplifyVW(geom_3857, %s)
                                    ELSE ST_SimplifyVW(geom_3857, %s)
                                END,
                                $1,
                                4096, 256, true
                            ) AS geom
                        FROM %I
                        WHERE geom_3857 && $1
                    ) tile
                $f$,
                    p_table,
                    pk_col,
                    p_z,
                    p_z, 1e-6 * POWER(2, 17 - p_z),
                    p_z, 1e-7 * POWER(2, 17 - p_z),
                    1e-8 * POWER(2, 17 - p_z),
                    p_table
                );
            END IF;

            -- 4️⃣ Execute query
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