import flow.redisom as rom
from collections import namedtuple

ColorGroup = namedtuple("ColorGroup", ["idx", "parent_color",
        "parent_color_group", "begin", "end"])

ColorGroup.size = property(lambda self: self.end - self.begin)
ColorGroup.colors = property(lambda self: range(self.begin, self.end))


def _color_group_enc(value):
    return rom.json_enc(value._asdict())


def _color_group_dec(value):
    return ColorGroup(**rom.json_dec(value))


class Token(rom.Object):
    data_type = rom.Property(rom.String)
    data = rom.Property(rom.Hash, value_encoder=rom.json_enc,
            value_decoder=rom.json_dec)

    net_key = rom.Property(rom.String)

    color = rom.Property(rom.Int)
    color_group_idx = rom.Property(rom.Int)

    @property
    def net(self):
        return rom.get_object(self.connection, self.net_key)

    def _on_create(self):
        try:
            self.data_type.value
        except rom.NotInRedisError:
            self.data_type = ""


class Net(rom.Object):
    next_color = rom.Property(rom.Int)
    next_color_group = rom.Property(rom.Int)
    color_groups = rom.Property(rom.Hash, value_encoder=rom.json_enc,
            value_decoder=rom.json_dec)

    color_marking = rom.Property(rom.Hash)
    group_marking = rom.Property(rom.Hash)

    num_places = rom.Property(rom.Int)
    num_transitions = rom.Property(rom.Int)
    num_tokens = rom.Property(rom.Int)

    @classmethod
    def create(cls, connection=None, name=None, place_names=[],
               transitions=[], place_arcs_out={},
               trans_arcs_out={}, key=None):

        #if key is None:
            #key = make_net_key()
        self = rom.create_object(cls, connection=connection, key=key)

        trans_arcs_in = {}
        for p, trans_set in place_arcs_out.iteritems():
            for t in trans_set:
                trans_arcs_in.setdefault(t, set()).add(p)

        place_arcs_in = {}
        for t, place_set in trans_arcs_out.iteritems():
            for p in place_set:
                place_arcs_in.setdefault(p, set()).add(t)

        self.num_places.value = len(place_names)
        self.num_transitions.value = len(transitions)

        #for i, pname in enumerate(place_names):
            #key = self.subkey("place/%d" % i)
            #self.place_class.create(connection=self.connection, key=key,
                    #name=pname, arcs_out=place_arcs_out.get(i, {}),
                    #arcs_in=place_arcs_in.get(i, {}))

        #for i, t in enumerate(transitions):
            #key = self.subkey("trans/%d" % i)
            #name = "" if t is None else t.name
            #action_key = None if t is None else t.key
            #trans = self.transition_class.create(self.connection, key,
                    #name=name, arcs_out=trans_arcs_out.get(i, {}),
                    #arcs_in=trans_arcs_in.get(i, {}))

            #if action_key is not None:
                #trans.action_key = action_key

        return self


    def color_group(self, idx):
        return self.color_groups[idx]

    def add_color_group(self, size, parent_color=None, parent_color_group=None):
        group_id = self.next_color_group.incr(1) - 1
        end = self.next_color.incr(size)
        begin = end - size

        cg = ColorGroup(idx=group_id, parent_color=parent_color,
                parent_color_group=parent_color_group, begin=begin, end=end)

        self.color_groups[group_id] = cg

        return cg

    def token_key(self, idx):
        return self.subkey("tok", idx)

    def token(self, idx):
        return Token(self.connection, self.token_key(idx))

    def _next_token_key(self):
        return self.token_key(self.num_tokens.incr() - 1)

    def create_token(self, color, color_group_idx, data=None, data_type=None):
        key = self._next_token_key()
        return Token.create(self.connection, key, net_key=self.key,
                data=data, data_type=data_type, color=color,
                color_group_idx=color_group_idx)


