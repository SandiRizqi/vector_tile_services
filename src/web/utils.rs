use std::f64::consts::PI;

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