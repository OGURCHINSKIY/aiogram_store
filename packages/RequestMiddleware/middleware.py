import typing
from aiogram import Bot
from aiogram.dispatcher.middlewares import BaseMiddleware, MiddlewareManager
from aiogram.types import base
from aiogram.utils.helper import HelperMode


class SkipRequest(Exception):
    pass


class RequestMiddleware(BaseMiddleware):
    bot: Bot
    orig_request: typing.Callable

    def setup(self, manager: MiddlewareManager):
        self._manager = manager
        self._configured = True
        bot = manager.dispatcher.bot
        self.orig_request = bot.request
        bot.request = self.request

    async def request(self, method: base.String,
                      data: typing.Optional[typing.Dict] = None,
                      files: typing.Optional[typing.Dict] = None, **kwargs
                      ) -> typing.Union[typing.List, typing.Dict, base.Boolean]:
        handler_name = f"on_{HelperMode.apply(method, HelperMode.snake_case)}"
        handler = getattr(self, handler_name, None)
        if handler:
            try:
                await handler(data, files, **kwargs)
            except SkipRequest:
                return {}
        return await self.orig_request(method, data, files)
