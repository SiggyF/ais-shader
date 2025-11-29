
import morecantile

def check_tiles():
    tms = morecantile.tms.get("WebMercatorQuad")
    us_bbox = (-125.0, 24.0, -66.0, 49.0)
    zoom = 5
    tiles = list(tms.tiles(*us_bbox, zooms=[zoom]))
    
    print(f"Total tiles: {len(tiles)}")
    xs = sorted(list(set(t.x for t in tiles)))
    print(f"X coordinates: {xs}")
    
    for x in xs:
        ys = sorted([t.y for t in tiles if t.x == x])
        print(f"X={x}: Y={ys}")

if __name__ == "__main__":
    check_tiles()
