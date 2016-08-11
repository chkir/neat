from unittest import TestCase
from neat.external import Stream


class TestExternal(TestCase):

    def test_external_empty(self):
        s = Stream(['a'])
        c = s.collect()

        s.push([dict(a=1)])
        self.assertEqual([dict(a=1)], c.fetch())

        s.push([dict(a=2)])
        s.push([dict(a=3)])
        self.assertEqual([dict(a=2), dict(a=3)], c.fetch())

    def test_external_join_func(self):
        def square(arg):
            return arg * arg

        s = Stream(['a'])
        f = s.join_func(['a'], 'b', square)
        c = f.collect()

        #import pdb; pdb.set_trace()
        s.push([dict(a=1)])
        self.assertEqual([dict(a=1, b=1)], c.fetch())

    def test_external_projection(self):
        s = Stream(['a', 'b'])
        p = s.projection(['a'])
        c = p.collect()

        s.push([dict(a=1,b=2)])
        self.assertEqual([dict(a=1)], c.fetch())

        s.push([dict(a=2,b=1)])
        self.assertEqual([dict(a=2)], c.fetch())

    def test_external_projection_dup(self):
        s = Stream(['a', 'b'])
        p = s.projection(['a'])
        c = p.collect()

        s.push([dict(a=1,b=1), dict(a=1,b=2)])
        self.assertEqual([dict(a=1)], c.fetch())

        s.push([dict(a=1,b=3)])
        self.assertEqual([], c.fetch())

    def test_external_natural_join(self):
        s1 = Stream(['a', 'b'])
        s2 = Stream(['b', 'c'])
        j = s1.natural_join(s2)
        c = j.collect()

        # first thing pushed has nothing to join to
        s1.push([dict(a=1,b=1)])
        self.assertEqual([], c.fetch())

        # matching row pushed emits join
        s2.push([dict(b=1, c=1)])
        self.assertEqual([dict(a=1,b=1,c=1)], c.fetch())

        # another thing pushed to s1 with a different join parameter
        # should also output nothing.
        s1.push([dict(a=1,b=2)])
        self.assertEqual([], c.fetch())

        # second matching row pushed emits join
        s2.push([dict(b=2, c=1)])
        self.assertEqual([dict(a=1,b=2,c=1)], c.fetch())

        # a push to a different first parameter should push out another
        # merge.
        s1.push([dict(a=2,b=2)])
        self.assertEqual([dict(a=2,b=2,c=1)], c.fetch())

        # a push to the second should now output _two_ matching rows
        s2.push([dict(b=2, c=2)])
        self.assertEqual([dict(a=2,b=2,c=2), dict(a=1,b=2,c=2)], c.fetch())

    def test_external_unnest(self):
        s = Stream(['id', 'stuffs'])
        u = s.unnest('stuffs', 'stuff')
        c = u.collect()

        s.push([dict(id=1,stuffs=[])])
        self.assertEqual([], c.fetch())

        s.push([dict(id=2,stuffs=(1,2,3))])
        self.assertEqual(
            sorted([
                dict(id=2,stuff=1),
                dict(id=2,stuff=2),
                dict(id=2,stuff=3)
            ]),
            sorted(c.fetch()))
