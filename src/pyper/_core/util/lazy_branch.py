from collections.abc import Iterable, AsyncIterable


class StopBranch:
    ...


class LazyBranch:
    def __init__(self, branch: Iterable) -> None:
        self.branch = branch
        self._stopped = False

    @property
    def stopped(self):
        return self._stopped

    def next_value(self):
        if (value := next(self.branch, StopBranch)) is StopBranch:
            self._stopped = True

        return value


class AsyncLazyBranch:
    def __init__(self, branch: AsyncIterable) -> None:
        self.branch = branch
        self._stopped = False

    @property
    def stopped(self):
        return self._stopped

    async def anext_value(self):
        # should use `anext` when bumped `requires-python` to 3.10.
        try:
            value = await self.branch.__anext__()
        except StopAsyncIteration:
            value = StopBranch
            self._stopped = True

        return value
