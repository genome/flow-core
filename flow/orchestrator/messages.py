from flow.protocol.message import Message


class CreateTokenMessage(Message):
    required_fields = {
            "net_key": basestring,
            "place_idx": int,
            "color": (int, long),
            "color_group_idx": (int, long),
    }

    optional_fields = {
            "data": object,
    }


class NotifyPlaceMessage(Message):
    required_fields = {
            "net_key": basestring,
            "place_idx": int,
            "color": (int, long),
    }


class NotifyTransitionMessage(Message):
    required_fields = {
            "net_key": basestring,
            "place_idx": int,
            "transition_idx": int,
            "token_idx": (int, long),
    }


class PlaceEntryObservedMessage(Message):
    required_fields = {
            "body": basestring,
    }
