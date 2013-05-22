import flow.redisom as rom
from place import Place
from collections import namedtuple

from twisted.internet import defer

_TOKEN_KEY = "tok"
_PLACE_KEY = "P"
_TRANSITION_KEY = "T"
_COLOR_KEY = "C"
_COLOR_GROUP_KEY = "CG"

_PUT_TOKEN_SCRIPT = """
local color_marking_key = KEYS[1]
local group_marking_key = KEYS[2]

local place_id = ARGV[1]
local token_idx = ARGV[2]
local color = ARGV[3]
local color_group_idx = ARGV[4]

local color_key = string.format("%s:%s", color, place_id)
local group_key = string.format("%s:%s", color_group_idx, place_id)

local set = redis.call('HSETNX', color_marking_key, color_key, token_idx)
if set == 0 then
    local existing_idx = redis.call('HGET', color_marking_key, color_key)
    if existing_idx ~= token_idx then
        return -1
    else
        return 0
    end
end

redis.call('HINCRBY', group_marking_key, group_key, 1)
return 0
"""

ColorGroup = namedtuple("ColorGroup", ["idx", "parent_color",
        "parent_color_group", "begin", "end"])

ColorGroup.size = property(lambda self: self.end - self.begin)
ColorGroup.colors = property(lambda self: range(self.begin, self.end))


def _color_group_enc(value):
    return rom.json_enc(value._asdict())


def _color_group_dec(value):
    return ColorGroup(**rom.json_dec(value))


def tagged_marking_key(tag, place_idx):
    return "%s:%s" % (tag, place_idx)


class PetriNetError(RuntimeError):
    pass


class PlaceNotFoundError(PetriNetError):
    pass


class ForeignTokenError(PetriNetError):
    pass


class Token(rom.Object):
    data = rom.Property(rom.Hash, value_encoder=rom.json_enc,
            value_decoder=rom.json_dec)

    net_key = rom.Property(rom.String)

    color = rom.Property(rom.Int)
    color_group_idx = rom.Property(rom.Int)
    index = rom.Property(rom.Int)

    @property
    def net(self):
        return rom.get_object(self.connection, self.net_key)


class Net(rom.Object):
    color_groups = rom.Property(rom.Hash, value_encoder=rom.json_enc,
            value_decoder=rom.json_dec)

    color_marking = rom.Property(rom.Hash)
    group_marking = rom.Property(rom.Hash)

    place_observer_keys = rom.Property(rom.Hash)

    counters = rom.Property(rom.Hash, value_encoder=int, value_decoder=int)

    _put_token_script = rom.Script(_PUT_TOKEN_SCRIPT)

    @property
    def num_places(self):
        return self.counters.get(_PLACE_KEY, 0)

    @property
    def num_transitions(self):
        return self.counters.get(_TRANSITION_KEY, 0)

    def add_place(self, name):
        idx = self._incr_counter(_PLACE_KEY) - 1
        return Place.create(self.connection, self.place_key(idx), index=idx)

    def add_transition(self, cls, *args, **kwargs):
        idx = self._incr_counter(_TRANSITION_KEY) - 1
        return cls.create(self.connection, self.transition_key(idx),
                *args, **kwargs)

    def _put_token(self, place_idx, token):
        if place_idx >= self.num_places:
            raise PlaceNotFoundError("Attempted to put token into place %s" %
                    place_idx)

        token_idx = token.index.value
        if token.key != self.token_key(token_idx):
            raise ForeignTokenError("Token %s cannot be placed in net %s" %
                    (token.key, self.key))

        keys = [self.color_marking.key, self.group_marking.key]
        args = [place_idx, token_idx, token.color.value,
                token.color_group_idx.value]

        rv = self._put_token_script(keys=keys, args=args)
        return rv

    def notify_place(self, place_idx, color, service_interfaces):
        key = tagged_marking_key(color, place_idx)
        token = self.color_marking.get(key)
        if token is not None:
            deferreds = []
            token_idx = token.index
            place = self.place(place_idx)
            place.first_token_timestamp.setnx()

            arcs = place.arcs_out.value
            orchestrator = service_interfaces['orchestrator']
            for transition_idx in arcs:
                df = orchestrator.notify_transition(self.key, transition_idx,
                    place_idx, token_idx)
                deferreds.append(df)

            return defer.DeferredList(deferreds)
        else:
            return defer.succeed(None)

    def notify_transition(self, transition_idx, place_idx, token_idx,
            service_interfaces):

        trans = self.transition(transition_idx)
        token = self.token(token_idx)
        return trans.notify(self, place_idx, token, service_interfaces) 

    def color_group(self, idx):
        return self.color_groups[idx]

    def add_color_group(self, size, parent_color=None, parent_color_group=None):
        group_id = self._incr_counter(_COLOR_GROUP_KEY) - 1
        end = self._incr_counter(_COLOR_KEY, size)
        begin = end - size

        cg = ColorGroup(idx=group_id, parent_color=parent_color,
                parent_color_group=parent_color_group, begin=begin, end=end)

        self.color_groups[group_id] = cg

        return cg

    def _incr_counter(self, which, size=1):
        return self.counters.incrby(which, size)

    def place_key(self, idx):
        return self.subkey(_PLACE_KEY, idx)

    def place(self, idx):
        return Place(self.connection, self.place_key(idx))

    def transition_key(self, idx):
        return self.subkey(_TRANSITION_KEy, idx)

    def transition(self, idx):
        return rom.get_object(self.connection, self.transition_key(idx))

    def token_key(self, idx):
        return self.subkey(_TOKEN_KEY, idx)

    def token(self, idx):
        return Token(self.connection, self.token_key(idx))

    def create_token(self, color, color_group_idx, data=None):
        idx = self._incr_counter(_TOKEN_KEY) - 1
        key = self.token_key(idx)
        return Token.create(self.connection, key, net_key=self.key,
                index=idx, data=data, color=color,
                color_group_idx=color_group_idx)
