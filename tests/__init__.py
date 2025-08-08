# Copyright 2022 New Vector Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import sqlite3
from asyncio import Future
from typing import Any, Awaitable, Callable, Optional, Tuple, TypeVar
from unittest.mock import MagicMock, Mock

from synapse.module_api import JsonDict, ModuleApi

from tchap_red_list import RedListManager

RV = TypeVar("RV")
TV = TypeVar("TV")


class SQLiteStore:
    """In-memory SQLite store. We can't just use a run_db_interaction function that opens
    its own connection, since we need to use the same connection for all queries in a
    test.
    """

    def __init__(self) -> None:
        self.conn = sqlite3.connect(":memory:")

    async def run_db_interaction(
        self, desc: str, f: Callable[..., RV], *args: Any, **kwargs: Any
    ) -> RV:
        cur = CursorWrapper(self.conn.cursor())
        try:
            res = f(cur, *args, **kwargs)
            self.conn.commit()
            return res
        except Exception:
            self.conn.rollback()
            raise


class MockEngine:
    supports_using_any_list = False


class CursorWrapper:
    """Wrapper around a SQLite cursor."""

    def __init__(self, cursor: sqlite3.Cursor) -> None:
        self.cur = cursor
        self.database_engine = MockEngine()

    def execute(self, sql: str, args: Any) -> None:
        self.cur.execute(sql, args)

    @property
    def description(self) -> Any:
        return self.cur.description

    @property
    def rowcount(self) -> Any:
        return self.cur.rowcount

    def fetchone(self) -> Any:
        return self.cur.fetchone()

    def fetchall(self) -> Any:
        return self.cur.fetchall()

    def __iter__(self) -> Any:
        return self.cur.__iter__()

    def __next__(self) -> Any:
        return self.cur.__next__()


def make_awaitable(result: TV) -> Awaitable[TV]:
    """
    Makes an awaitable, suitable for mocking an `async` function.
    This uses Futures as they can be awaited multiple times so can be returned
    to multiple callers.
    """
    future = Future()  # type: ignore
    future.set_result(result)
    return future


async def invalidate_cache(cached_func, keys):
    cached_func.invalidate(keys)


async def _setup_synapse_db(store: SQLiteStore) -> None:
    """Create a table mocking the one created by synapse-email-account-validity,
    except only with the columns used by the red list module, and populate it.

    Args:
        store: the store to use to create and populate the table.
    """
    txn = store.conn.cursor()

    txn.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            name text,
            password_hash text,
            creation_ts bigint,
            admin smallint DEFAULT 0 NOT NULL,
            upgrade_ts bigint,
            is_guest smallint DEFAULT 0 NOT NULL,
            appservice_id text,
            consent_version text,
            consent_server_notice_sent text,
            user_type text,
            deactivated smallint DEFAULT 0 NOT NULL,
            shadow_banned boolean,
            consent_ts bigint
        );
        """,
        (),
    )

    txn.execute(
        """
        CREATE TABLE IF NOT EXISTS email_account_validity(
            user_id TEXT PRIMARY KEY,
            expiration_ts_ms BIGINT NOT NULL
        )
        """,
        (),
    )

    store.conn.commit()


async def create_module(
    config: Optional[JsonDict] = None,
) -> Tuple[RedListManager, Mock, SQLiteStore]:
    """Create an instance of the module.

    Args:
        config: the config to give the module, if any.

    Returns:
        The instance of the module, the mock for the module API so the tests can check
        its calls, and the store used by the module so the test can e.g. maintain a dummy
        account validity table.
    """
    store = SQLiteStore()

    # Create a mock based on the ModuleApi spec, but override some mocked functions
    # because some capabilities are needed for running the tests.
    hs = MagicMock()
    module_api = Mock(spec=ModuleApi(hs, None))
    module_api.run_db_interaction.side_effect = store.run_db_interaction
    module_api.update_room_membership.return_value = make_awaitable(None)
    module_api.invalidate_cache.side_effect = invalidate_cache

    # If necessary, give parse_config some configuration to parse.
    raw_config = config if config is not None else {}
    parsed_config = RedListManager.parse_config(raw_config)

    # Set up the database table in a separate step, to ensure it's ready to be used when
    # we run the tests.
    module = RedListManager(parsed_config, module_api, setup_db=False)
    await _setup_synapse_db(store)
    await module._setup_db()

    # Instantiating the module will set up the database, which will call
    # run_db_interaction. We don't want to count this call in tests, so we reset its
    # call history.
    module_api.run_db_interaction.reset_mock()

    return module, module_api, store
