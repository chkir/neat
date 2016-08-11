from unittest import TestCase
from neat.relation import Relation
from neat.tiles import tile, node_tiles

class TestNeat(TestCase):

    def test_node_tiles(self):
        tile0 = tile(0, 0)
        tile1 = tile(1, 1)

        nodes = Relation(['id', 'version', 'lon', 'lat'],
                         [(1, 1, 0, 0),
                          (1, 2, 1, 1),
                          (2, 1, 0, 0)])
        tiles = node_tiles(nodes).\
                projection(['tile', 'id', 'version', 'lon', 'lat'])

        self.assertEqual(Relation(['tile', 'id', 'version', 'lon', 'lat'],
                                  [(tile0, 1, 1, 0, 0),
                                   (tile0, 1, 2, 1, 1),
                                   (tile0, 2, 1, 0, 0),
                                   (tile1, 1, 1, 0, 0),
                                   (tile1, 1, 2, 1, 1)]),
                         tiles)
