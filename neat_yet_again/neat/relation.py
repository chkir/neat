# a relation in the algebraic data model

class Relation(object):

    def __init__(self, attribute_names, tuples=None):
        self.attribute_names = attribute_names
        if tuples:
            for t in tuples:
                assert len(t) == len(self.attribute_names), \
                    "Relation tuple %r incompatible with attributes %r." \
                    % (t, self.attribute_names)
            if isinstance(tuples, set):
                self.tuples = tuples
            else:
                self.tuples = set(tuples)
        else:
            self.tuples = set()

    def union(self, other):
        self._assert_compatible(other, "union")
        return Relation(self.attribute_names, self.tuples.union(other.tuples))

    def intersection(self, other):
        self._assert_compatible(other, "intersection")
        return Relation(self.attribute_names, self.tuples.intersection(other.tuples))

    def difference(self, other):
        self._assert_compatible(other, "difference")
        return Relation(self.attribute_names, self.tuples.difference(other.tuples))

    def projection(self, new_attribute_names):
        indices = list()
        for n in new_attribute_names:
            assert n in self.attribute_names, \
                "Can't project attribute %r, not in relation %r" \
                % (n, self.attribute_names)
            indices.append(self.attribute_names.index(n))

        new_tuples = set()
        for t in self.tuples:
            new_t = tuple([t[i] for i in indices])
            new_tuples.add(new_t)

        return Relation(new_attribute_names, new_tuples)

    def selection(self, pred):
        new_tuples = set()
        for t in self.tuples:
            if pred(t):
                new_tuples.add(t)

        return Relation(self.attribute_names, new_tuples)

    def rename(self, new_attribute_names):
        assert len(new_attribute_names) == len(self.attribute_names), \
            "New attribute names wrong length: len(%r) != len(%r)" \
            % (new_attribute_names, self.attribute_names)

        return Relation(new_attribute_names, self.tuples)

    def join_func(self, arg_names, result_name, func):
        idx = self._indices_for(arg_names)

        new_tuples = set()
        for t in self.tuples:
            args = [t[i] for i in idx]
            result = func(*args)
            new_tuples.add(tuple(list(t) + [result]))

        return Relation(self.attribute_names + [result_name], new_tuples)

    def natural_join(self, other):
        self_attr_set = set(self.attribute_names)
        other_attr_set = set(other.attribute_names)

        join_attrs = self_attr_set & other_attr_set
        additional_attrs = list(other_attr_set - self_attr_set)

        assert len(join_attrs) > 0, \
            "Natural join needs at least one shared attribute " \
            "between %r and %r" \
            % (self.attribute_names, other.attribute_names)

        join_idx = zip(self._indices_for(join_attrs),
                       other._indices_for(join_attrs))

        additional_idx = other._indices_for(additional_attrs)

        new_tuples = set()
        for t1 in self.tuples:
            for t2 in other.tuples:
                match = True
                for i1, i2 in join_idx:
                    if t1[i1] != t2[i2]:
                        match = False
                        break

                if match:
                    new_t = tuple(list(t1) + [t2[i] for i in additional_idx])
                    new_tuples.add(new_t)

        return Relation(self.attribute_names + additional_attrs, new_tuples)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.attribute_names == other.attribute_names and \
                self.tuples == other.tuples
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return "Relation(%r, %r)" % (self.attribute_names, list(self.tuples))

    def _indices_for(self, attributes):
        indices = list()
        for n in attributes:
            indices.append(self.attribute_names.index(n))
        return indices

    def _assert_compatible(self, other, op_name):
        assert self.attribute_names == other.attribute_names, \
            "Relation attributes incompatible in %s: %r != %r" \
            % (op_name, self.attribute_names, other.attribute_names)
