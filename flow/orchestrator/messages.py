from flow.protocol.message import Message


class CreateTokenMessage(Message):
    required_fields = {
            "net_key": basestring,
            "place_idx": int,
    }

    optional_fields = {
            "create_token_kwargs": dict,
    }


class NotifyPlaceMessage(Message):
    required_fields = {
            "net_key": basestring,
            "place_idx": int,
    }

    optional_fields = {
            "token_color": int,
    }


class NotifyTransitionMessage(Message):
    required_fields = {
            "net_key": basestring,
            "place_idx": int,
            "transition_idx": int,
    }

    optional_fields = {
            "token_color": int,
    }

class PlaceEntryObservedMessage(Message):
    required_fields = {
            "body": basestring,
    }

    optional_fields = {
    }

