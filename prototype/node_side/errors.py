"""The reference Node side's rejection: an ErrorCode-style `code` plus a message (what Node's
internal endpoints answer with)."""


class EditRejected(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
