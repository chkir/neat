from neat.relation import Relation

def tile(lon, lat):
    return "%d/%d" % (int(lon), int(lat))


def node_tiles_to_id(nodes):
    nodes_with_tile = nodes.join_func(['lon', 'lat'], 'tile', tile)
    tile_to_id = nodes_with_tile.projection(['tile', 'id'])
    return tile_to_id


def node_tiles(nodes):
    tile_to_id = node_tiles_to_id(nodes)
    tiles = tile_to_id.natural_join(nodes)
    return tiles

def way_tiles_to_id(nodes, ways):
    node_tiles = node_tiles_to_id(nodes).rename(['tile', 'node_id'])
    way_nodes = ways.unnest('nodes', 'node_id').projection(['id', 'node_id'])
    tile_to_id = node_tiles.natural_join(way_nodes).projection(['tile', 'id'])
    return tile_to_id

def way_tiles(nodes, ways):
    tile_to_id = way_tiles_to_id(nodes, ways)
    tiles = tile_to_id.natural_join(ways)
    return tiles
