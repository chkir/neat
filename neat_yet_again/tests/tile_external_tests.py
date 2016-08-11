from unittest import TestCase
from neat.external import Stream
from neat.tiles import tile, node_tiles, way_tiles

def tile(lon, lat):
    return "%d/%d" % (int(lon), int(lat))

class TestNeat(TestCase):

    def test_node_tiles(self):
        tile0 = tile(0, 0)
        tile1 = tile(1, 1)

        nodes = Stream(['id', 'version', 'lon', 'lat'])
        tiles = node_tiles(nodes).\
                projection(['tile', 'id', 'version', 'lon', 'lat']).\
                collect()

        nodes.push([dict(id=1, version=1, lon=0, lat=0)])
        self.assertEqual(
            [dict(id=1, version=1, lon=0, lat=0, tile=tile0)],
            tiles.fetch())

        nodes.push([dict(id=1, version=2, lon=1, lat=1)])
        self.assertEqual(
            sorted([dict(id=1, version=2, lon=1, lat=1, tile=tile0),
                    dict(id=1, version=1, lon=0, lat=0, tile=tile1),
                    dict(id=1, version=2, lon=1, lat=1, tile=tile1)]),
            sorted(tiles.fetch()))

        nodes.push([dict(id=2, version=1, lon=0, lat=0)])
        self.assertEqual(
            [dict(id=2, version=1, lon=0, lat=0, tile=tile0)],
            tiles.fetch())

    def test_way_tiles(self):
        tile0 = tile(0, 0)
        tile1 = tile(1, 1)

        nodes = Stream(['id', 'version', 'lon', 'lat'])
        ways = Stream(['id', 'version', 'nodes'])
        tiles = way_tiles(nodes, ways).\
                projection(['tile', 'id', 'version']).\
                collect()

        nodes.push([dict(id=1,version=1,lon=0,lat=0),
                    dict(id=2,version=1,lon=0,lat=0)])
        ways.push([dict(id=1,version=1,nodes=(1,2))])
        self.assertEqual(
            [dict(tile=tile0,id=1,version=1)],
            tiles.fetch())
