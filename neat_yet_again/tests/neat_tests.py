from unittest import TestCase
from neat.relation import Relation

class TestRelation(TestCase):

    def test_relation_union_empty(self):
        r1 = Relation(['a'])
        r2 = Relation(['a'])
        r3 = r1.union(r2)
        self.assertEqual(Relation(['a']), r3)

    def test_relation_union_left(self):
        r1 = Relation(['a'], [(1,)])
        r2 = Relation(['a'])
        r3 = r1.union(r2)
        self.assertEqual(Relation(['a'], [(1,)]), r3)

    def test_relation_union_right(self):
        r1 = Relation(['a'])
        r2 = Relation(['a'], [(1,)])
        r3 = r1.union(r2)
        self.assertEqual(Relation(['a'], [(1,)]), r3)

    def test_relation_union_both(self):
        r1 = Relation(['a'], [(1,)])
        r2 = Relation(['a'], [(1,)])
        r3 = r1.union(r2)
        self.assertEqual(Relation(['a'], [(1,)]), r3)

    def test_relation_union_different(self):
        r1 = Relation(['a'], [(1,)])
        r2 = Relation(['a'], [(2,)])
        r3 = r1.union(r2)
        self.assertEqual(Relation(['a'], [(1,), (2,)]), r3)

    def test_relation_intersection_empty(self):
        r1 = Relation(['a'])
        r2 = Relation(['a'])
        r3 = r1.intersection(r2)
        self.assertEqual(Relation(['a']), r3)

    def test_relation_intersection_left(self):
        r1 = Relation(['a'], [(1,)])
        r2 = Relation(['a'])
        r3 = r1.intersection(r2)
        self.assertEqual(Relation(['a']), r3)

    def test_relation_intersection_right(self):
        r1 = Relation(['a'])
        r2 = Relation(['a'], [(1,)])
        r3 = r1.intersection(r2)
        self.assertEqual(Relation(['a']), r3)

    def test_relation_intersection_both(self):
        r1 = Relation(['a'], [(1,)])
        r2 = Relation(['a'], [(1,)])
        r3 = r1.intersection(r2)
        self.assertEqual(Relation(['a'], [(1,)]), r3)

    def test_relation_intersection_different(self):
        r1 = Relation(['a'], [(1,)])
        r2 = Relation(['a'], [(2,)])
        r3 = r1.intersection(r2)
        self.assertEqual(Relation(['a']), r3)

    def test_relation_difference_empty(self):
        r1 = Relation(['a'])
        r2 = Relation(['a'])
        r3 = r1.difference(r2)
        self.assertEqual(Relation(['a']), r3)

    def test_relation_difference_left(self):
        r1 = Relation(['a'], [(1,)])
        r2 = Relation(['a'])
        r3 = r1.difference(r2)
        self.assertEqual(Relation(['a'], [(1,)]), r3)

    def test_relation_difference_right(self):
        r1 = Relation(['a'])
        r2 = Relation(['a'])
        r3 = r1.difference(r2)
        self.assertEqual(Relation(['a']), r3)

    def test_relation_difference_both(self):
        r1 = Relation(['a'], [(1,)])
        r2 = Relation(['a'], [(1,)])
        r3 = r1.difference(r2)
        self.assertEqual(Relation(['a']), r3)

    def test_relation_difference_different(self):
        r1 = Relation(['a'], [(1,)])
        r2 = Relation(['a'], [(2,)])
        r3 = r1.difference(r2)
        self.assertEqual(Relation(['a'], [(1,)]), r3)

    def test_relation_projection(self):
        r1 = Relation(['a', 'b', 'c'], [(1, 2, 3)])
        r2 = r1.projection(['b'])
        self.assertEqual(Relation(['b'], [(2,)]), r2)

    def test_relation_selection(self):
        r1 = Relation(['a'], [(1,), (2,), (3,), (4,), (5,)])
        r2 = r1.selection(lambda t: (t[0] % 2) == 0)
        self.assertEqual(Relation(['a'], [(2,), (4,)]), r2)

    def test_relation_rename(self):
        r1 = Relation(['a'], [(1,)])
        r2 = r1.rename(['b'])
        self.assertEqual(Relation(['b'], [(1,)]), r2)

    def test_relation_natural_join(self):
        r1 = Relation(['a', 'b'], [(1,1), (1,2), (2,2)])
        r2 = Relation(['b', 'c'], [(1,3), (2,4)])
        r3 = r1.natural_join(r2)
        self.assertEqual(Relation(['a','b','c'], [(1,1,3), (1,2,4), (2,2,4)]),
                         r3)

    def test_relation_natural_join_2(self):
        r1 = Relation(['a', 'b'], [(1,1), (1,2), (2,3)])
        r2 = Relation(['b', 'c'], [(1,3), (2,4)])
        r3 = r1.natural_join(r2)
        self.assertEqual(Relation(['a','b','c'], [(1,1,3), (1,2,4)]), r3)

    def test_relation_natural_join_3(self):
        r1 = Relation(['a', 'b'], [(1,1), (1,2), (2,1)])
        r2 = Relation(['b', 'c'], [(1,3), (1,4)])
        r3 = r1.natural_join(r2)
        self.assertEqual(
            Relation(['a','b','c'], [(1,1,3), (1,1,4), (2,1,3), (2,1,4)]), r3)

    def test_relation_join_func(self):
        r1 = Relation(['a', 'b'], [(1, 1), (1, 2), (2, 2), (1, 10)])
        def mult(a, b):
            return a*b
        r2 = r1.join_func(['a', 'b'], 'c', mult)
        self.assertEqual(
            Relation(['a', 'b', 'c'], [(1, 1, 1), (1, 2, 2), (2, 2, 4),
                                       (1, 10, 10)]),
            r2)
