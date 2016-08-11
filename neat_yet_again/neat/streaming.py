class Collector(object):
    """
    Collects the output of a Stream and saves it in a `list` until `fetch()` is
    called, which returns the list and resets it. This is useful for testing,
    or to "tap" into Streams to see what they're doing.
    """

    def __init__(self):
        self._items = list()

    def push(self, items):
        self._items.extend(items)

    def fetch(self):
        items = self._items
        self._items = list()
        return items


class HalfJoinStream(object):
    """
    Half of a JoiningStream, which has the `push()` method needed for an
    upstream Stream to insert data into it, which is combined with information
    about whether it's the left or right half of a join, then pushed down to
    the JoiningStream.
    """

    def __init__(self, pos, parent):
        self._pos = pos
        self._parent = parent

    def push(self, items):
        return self._parent.push(self._pos, items)


class JoiningStream(object):
    """
    A Stream interface object which implements a natural join between two
    Streams.
    """

    def __init__(self, left, right):
        self._join = dict()

        self.left = HalfJoinStream(0, self)
        self.right = HalfJoinStream(1, self)

        left_attr_set = set(left.attr_names)
        right_attr_set = set(right.attr_names)

        self._join_attrs = list(left_attr_set & right_attr_set)
        self._left_attrs = list(left_attr_set - right_attr_set)
        self._right_attrs = list(right_attr_set - left_attr_set)

        assert len(self._join_attrs) > 0, \
            "Natural join needs at least one shared attribute " \
            "between %r and %r" \
            % (left.attr_names, right.attr_names)

        self._listeners = list()

    def attrs(self):
        return self._left_attrs + self._join_attrs + self._right_attrs

    def push(self, pos, items):
        output_items = list()

        for item in items:
            idx = tuple([item[k] for k in self._join_attrs])
            l, r = self._join.get(idx, (set(), set()))

            if pos == 0:
                # left
                left_data = tuple([item[k] for k in self._left_attrs])
                l, r, out = self._push(l, self._left_attrs, left_data, idx,
                                       r, self._right_attrs)

            else:
                # right
                right_data = tuple([item[k] for k in self._right_attrs])
                r, l, out = self._push(r, self._right_attrs, right_data, idx,
                                       l, self._left_attrs)

            output_items.extend(out)
            self._join[idx] = (l, r)

        for listener in self._listeners:
            listener.push(output_items)

    def _push(self, existing_set, existing_attrs, new_vals, join_vals,
              other_set, other_attrs):
        out = list()

        if new_vals not in existing_set:
            base_val = dict()

            for k, v in zip(existing_attrs, new_vals):
                base_val[k] = v
            for k, v in zip(self._join_attrs, join_vals):
                base_val[k] = v

            for o in other_set:
                val = base_val.copy()
                for k, v in zip(other_attrs, o):
                    val[k] = v
                out.append(val)

            existing_set.add(new_vals)

        return existing_set, other_set, out


class Stream(object):
    """
    Streaming relational operations. Each instance of Stream may transform the
    tuples and `push()` them down to listeners. In this way, a network of
    Streams can perform operations with limited local state.
    """

    def __init__(self, attr_names, push_func=None):
        self.attr_names = attr_names
        self._push_func = push_func
        self._listeners = list()

    def push(self, items):
        if self._push_func is not None:
            items = self._push_func(self, items)

        #import sys; print>>sys.stderr,"PUSH: %r" % items
        for listener in self._listeners:
            listener.push(items)

    def collect(self):
        collector = Collector()
        self._listeners.append(collector)
        return collector

    def union(self, other):
        self._assert_compatible(other, "union")
        # TODO
        raise StandardError("Stream.union not implemented")

    def intersection(self, other):
        self._assert_compatible(other, "intersection")
        # TODO
        raise StandardError("Stream.intersection not implemented")

    def difference(self, other):
        self._assert_compatible(other, "difference")
        # TODO
        raise StandardError("Stream.difference not implemented")

    def projection(self, new_attr_names):
        emitted = set()

        def push_func(stream, items):
            new_items = list()
            for item in items:
                new_item = dict([[k, item[k]] for k in new_attr_names])
                new_item_hashable = tuple(sorted(new_item.items()))
                if new_item_hashable not in emitted:
                    emitted.add(new_item_hashable)
                    new_items.append(new_item)
            return new_items

        s = Stream(new_attr_names, push_func)
        self._listeners.append(s)
        return s

    def selection(self, pred):
        # TODO
        raise StandardError("Stream.selection not implemented")

    def rename(self, new_attr_names):
        names = zip(new_attr_names, self.attr_names)

        def push_func(stream, items):
            new_items = list()
            for item in items:
                new_item = dict([(new, item[old]) for (new, old) in names])
                new_items.append(new_item)
            return new_items

        s = Stream(new_attr_names, push_func)
        self._listeners.append(s)
        return s

    def unnest(self, attr, unnested_attr):
        seen = set()

        def push_func(stream, items):
            new_items = list()
            for item in items:
                for val in item[attr]:
                    new_item = item.copy()
                    new_item.pop(attr)
                    new_item[unnested_attr] = val
                    hashable = tuple(sorted(new_item.items()))
                    if hashable not in seen:
                        seen.add(hashable)
                        new_items.append(new_item)
            return new_items

        replace_idx = self.attr_names.index(attr)
        attrs = list(self.attr_names)
        attrs[replace_idx] = unnested_attr
        s = Stream(attrs, push_func)
        self._listeners.append(s)
        return s

    def join_func(self, arg_names, result_name, func):
        def push_func(stream, items):
            new_items = list()
            for item in items:
                assert isinstance(item, dict), "Item %r should be dict" % item
                new_item = item.copy()
                args = [item[arg] for arg in arg_names]
                new_item[result_name] = func(*args)
                new_items.append(new_item)
            return new_items

        s = Stream(self.attr_names + [result_name], push_func)
        self._listeners.append(s)
        return s

    def natural_join(self, other):
        js = JoiningStream(self, other)

        self._listeners.append(js.left)
        other._listeners.append(js.right)

        s = Stream(js.attrs())
        js._listeners.append(s)
        return s

    def _indices_for(self, attrs):
        indices = list()
        for n in attrs:
            indices.append(self.attr_names.index(n))
        return indices

    def _assert_compatible(self, other, op_name):
        assert self.attr_names == other.attr_names, \
            "Stream attributes incompatible in %s: %r != %r" \
            % (op_name, self.attr_names, other.attr_names)
