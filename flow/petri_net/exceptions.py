class PetriNetError(RuntimeError):
    pass


class TokenError(PetriNetError):
    pass

class BadTokenDataError(TokenError):
    pass

class ForeignTokenError(TokenError):
    pass


class PlaceNotFoundError(PetriNetError):
    pass
