from flow.petri_net import lua
from flow.petri_net.color import ColorGroup
from flow.petri_net.color import color_group_enc, color_group_dec
from flow.petri_net.exceptions import ForeignTokenError, PlaceNotFoundError
from flow.petri_net.place import Place
from flow.petri_net.token import Token
from twisted.internet import defer
from uuid import uuid4


import base64
import flow.redisom as rom


_TOKEN_KEY = "t"
_PLACE_KEY = "P"
_TRANSITION_KEY = "T"
_COLOR_KEY = "C"
_COLOR_GROUP_KEY = "G"


class Net(rom.Object):
    # XXX Do we need to keep these around?
    # add group_id?
    required_constants = ["environment", "user_id", "working_directory"]

    name = rom.Property(rom.String)
    color_groups = rom.Property(rom.Hash, value_encoder=color_group_enc,
            value_decoder=color_group_dec)

    color_marking = rom.Property(rom.Hash, value_encoder=int, value_decoder=int)
    group_marking = rom.Property(rom.Hash, value_encoder=int, value_decoder=int)

    place_observer_keys = rom.Property(rom.Hash)

    counters = rom.Property(rom.Hash, value_encoder=int, value_decoder=int)

    variables = rom.Property(rom.Hash, value_encoder=rom.json_enc,
            value_decoder=rom.json_dec)
    _constants = rom.Property(rom.Hash, value_encoder=rom.json_enc,
            value_decoder=rom.json_dec)

    _put_token_script = rom.Script(lua.load('put_token'))


    @classmethod
    def make_default_key(cls):
        return base64.b64encode(uuid4().bytes)[:-2]

    @property
    def num_places(self):
        return self.counters.get(_PLACE_KEY, 0)

    @num_places.setter
    def num_places(self, new_value):
        if self.counters.setnx(_PLACE_KEY, new_value) == 0:
            raise ValueError('Tried to overwrite num_places')

    @property
    def num_transitions(self):
        return self.counters.get(_TRANSITION_KEY, 0)

    @num_transitions.setter
    def num_transitions(self, new_value):
        if self.counters.setnx(_TRANSITION_KEY, new_value) == 0:
            raise ValueError('Tried to overwrite num_transitions')


    def constant(self, key, default=None):
        return self._constants.get(key, default)

    def set_constant(self, key, value):
        if self._constants.setnx(key, value) == 0:
            raise TypeError("Tried to overwrite constant %s in net %s" %
                    (key, self.key))

    def set_variable(self, key, value):
        self.variables[key] = value

    def variable(self, key, default=None):
        return self.variables.get(key, default)


    def add_place(self, name):
        idx = self._incr_counter(_PLACE_KEY) - 1
        return Place.create(self.connection, self.place_key(idx),
                index=idx, name=name)

    def add_transition(self, cls, *args, **kwargs):
        idx = self._incr_counter(_TRANSITION_KEY) - 1
        return cls.create(self.connection, self.transition_key(idx),
                *args, **kwargs)

    def put_token(self, place_idx, token):
        if place_idx >= self.num_places:
            raise PlaceNotFoundError("Attempted to put token into place %s "
                    "(%d places exist)" % (place_idx, self.num_places))

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
        key = self.marking_key(color, place_idx)
        token_idx = self.color_marking.get(key)
        if token_idx is not None:
            deferreds = []
            place = self.place(place_idx)
            place.first_token_timestamp.setnx()

            arcs = place.arcs_out.value
            orchestrator = service_interfaces['orchestrator']
            for transition_idx in arcs:
                df = orchestrator.notify_transition(net_key=self.key,
                        transition_idx=transition_idx, place_idx=place_idx,
                        token_idx=token_idx)
                deferreds.append(df)

            return defer.DeferredList(deferreds)
        else:
            return defer.succeed(None)

    def notify_transition(self, transition_idx, place_idx, token_idx,
            service_interfaces):
        trans = self.transition(transition_idx)
        token = self.token(token_idx)
        color_descriptor = token.color_descriptor

        consume_rv = trans.consume_tokens(place_idx, color_descriptor,
                self.color_marking.key, self.group_marking.key)

        if consume_rv == 0:
            new_tokens, deferred = trans.fire(self,
                    color_descriptor, service_interfaces)
            colors = [x.color.value for x in new_tokens]
            trans.push_tokens(self, color_descriptor, new_tokens)
            trans.notify_places(self.key, colors, service_interfaces)

            return deferred
        else:
            return defer.succeed(None)

    def color_group(self, idx):
        return self.color_groups[idx]

    def set_initial_color(self, initial_color):
        if self.counters.setnx(_COLOR_KEY, initial_color) == 0:
            raise ValueError("Cannot set initial color, since "
                    "color has already been incremented")

    def add_color_group(self, size, parent_color=None,
            parent_color_group_idx=None):
        group_id = self._incr_counter(_COLOR_GROUP_KEY) - 1
        end = self._incr_counter(_COLOR_KEY, size)
        begin = end - size

        cg = ColorGroup(idx=group_id, parent_color=parent_color,
                parent_color_group_idx=parent_color_group_idx,
                begin=begin, end=end)

        self.color_groups[group_id] = cg

        return cg

    def _incr_counter(self, which, size=1):
        return self.counters.incrby(which, size)

    def place_key(self, idx):
        return self.subkey(_PLACE_KEY, idx)

    def place(self, idx):
        return Place(self.connection, self.place_key(idx))

    def transition_key(self, idx):
        return self.subkey(_TRANSITION_KEY, idx)

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

    def create_put_notify(self, place_idx, service_interfaces,
            color, color_group_idx, data=None):
        token = self.create_token(color, color_group_idx, data)
        self.put_token(place_idx, token)
        return self.notify_place(place_idx, color, service_interfaces)

    @staticmethod
    def marking_key(tag, place_idx):
        return "%s:%s" % (tag, place_idx)
