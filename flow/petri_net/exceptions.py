class PetriNetError(RuntimeError):
    pass


class ForeignTokenError(PetriNetError):
    pass


class PlaceNotFoundError(PetriNetError):
    pass
