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
import logging
import time
from typing import Any, Dict, List, Optional, Tuple, Union, Set

import attr
from synapse.module_api import (
    DatabasePool,
    JsonDict,
    LoggingTransaction,
    ModuleApi,
    UserProfile,
    cached,
    run_in_background,
)
from synapse.module_api.errors import ConfigError, SynapseError

logger = logging.getLogger(__name__)

ACCOUNT_DATA_TYPE = "im.vector.hide_profile"


@attr.s(auto_attribs=True, frozen=True)
class RedListManagerConfig:
    discovery_room: Optional[str] = None
    use_email_account_validity: bool = False


class RedListManager:
    def __init__(
        self, config: RedListManagerConfig, api: ModuleApi, setup_db: bool = True
    ):
        # Keep a reference to the config and Module API
        self._api = api
        self._config = config
        self._state_storage_controller = self._api._hs.get_storage_controllers().state

        # Register callbacks
        self._api.register_account_data_callbacks(
            on_account_data_updated=self.update_red_list_status,
        )

        self._api.register_spam_checker_callbacks(
            check_username_for_spam=self.check_user_in_red_list,
        )

        self._api.register_cached_function(self._get_user_status)

        if setup_db:
            # Set up the storage layer
            # We run this in the background because there's no other way to run async code
            # in __init__. However, this means we might have a race if something causes
            # the table to be accessed before it's fully created.
            run_in_background(self._setup_db)

        if self._config.use_email_account_validity:
            self._api.looping_background_call(self._add_expired_users, 60 * 60 * 1000)
            self._api.looping_background_call(
                self._remove_renewed_users, 60 * 60 * 1000
            )

        if self._config.discovery_room:
            self._api.looping_background_call(
                self._update_discovery_room, 60 * 60 * 1000
            )

    @staticmethod
    def parse_config(config: Dict[str, Any]) -> RedListManagerConfig:
        return RedListManagerConfig(**config)

    async def update_red_list_status(
        self,
        user_id: str,
        room_id: Optional[str],
        account_data_type: str,
        content: JsonDict,
    ) -> None:
        """Update a user's status in the red list when their account data changes.
        Implements the on_account_data_updated account data callback.
        """
        logger.debug(f"Update Red List {user_id}={content}")
        if account_data_type != ACCOUNT_DATA_TYPE:
            return

        # Compare what status (in the list, not in the list) the user wants to have with
        # what it already has. If they're the same, don't do anything more.
        desired_status = bool(content.get("hide_profile"))
        current_status, because_expired = await self._get_user_status(user_id)

        if current_status == desired_status:
            if because_expired is True:
                # There can be a delay between the user renewing their account (from an
                # account validity perspective) and the module actually picking up the
                # renewal, during which the user might decide to add their profile to the
                # red list.
                # In this case, we want to clear the because_expired flag so the user
                # isn't removed from the red list next time we check account validity
                # data.
                await self._make_addition_permanent(user_id)
        else:
            if desired_status is True:
                await self._add_to_red_list(user_id)
            else:
                await self._remove_from_red_list(user_id)

    async def _maybe_change_membership_in_discovery_room(
        self, user_id: str, membership: str
    ) -> None:
        """Change a user's membership in the discovery room.

        Does nothing if no discover room has been configured.

        Args:
            user_id: the user to change the membership of.
            membership: the membership to set for this user.
        """
        if self._config.discovery_room is None:
            return

        await self._api.update_room_membership(
            sender=user_id,
            target=user_id,
            room_id=self._config.discovery_room,
            new_membership=membership,
        )

    async def check_user_in_red_list(self, user_profile: UserProfile) -> bool:
        """Check if a user should be in the red list, which means they need to be hidden
        from local user directory search results.
        Implements the check_username_for_spam spam checker callback.
        """
        user_in_red_list, _ = await self._get_user_status(user_profile["user_id"])
        logger.debug(f"User {user_profile['user_id']} in red list={user_in_red_list}")
        return user_in_red_list

    async def _add_expired_users(self) -> None:
        """Retrieve all expired users and adds them to the red list."""

        def add_expired_users_txn(txn: LoggingTransaction) -> List[str]:
            # Retrieve all the expired users.
            sql = """
            SELECT user_id FROM email_account_validity WHERE expiration_ts_ms <= ?
            """

            now_ms = int(time.time() * 1000)
            txn.execute(sql, (now_ms,))
            expired_users_rows = txn.fetchall()

            expired_users = [row[0] for row in expired_users_rows]

            # Figure out which users are in the red list.
            # We could also inspect the cache on self._get_user_status and only query the
            # status of the users that aren't cached, but
            #   1) it's probably digging too much into Synapse's internals (i.e. it could
            #      easily break without warning)
            #   2) it's not clear that there would be such a huge perf gain from doing
            #      things this way.
            red_list_users_rows = DatabasePool.simple_select_many_txn(
                txn=txn,
                table="tchap_red_list",
                column="user_id",
                iterable=expired_users,
                keyvalues={},
                retcols=["user_id"],
            )

            # Figure out which users we need to add to the red list by looking up whether
            # they're already in it.
            users_in_red_list = [row[0] for row in red_list_users_rows]
            users_to_add = [
                user for user in expired_users if user not in users_in_red_list
            ]

            # Add all the expired users not in the red list.
            sql = """
            INSERT INTO tchap_red_list(user_id, because_expired) VALUES(?, ?)
            """
            for user in users_to_add:
                txn.execute(sql, (user, True))

            return users_to_add

        users_added = await self._api.run_db_interaction(
            "tchap_red_list_hide_expired_users",
            add_expired_users_txn,
        )

        # Make the expired users leave the discovery room if there's one.
        for user in users_added:
            await self._api.invalidate_cache(self._get_user_status, (user,))
            await self._maybe_change_membership_in_discovery_room(user, "leave")

    async def _remove_renewed_users(self) -> None:
        """Remove users from the red list if they have been added by _add_expired_users
        and have since then renewed their account.
        """

        def remove_renewed_users_txn(txn: LoggingTransaction) -> List[str]:
            # Retrieve the list of users we have previously added because their account
            # expired.
            rows = DatabasePool.simple_select_list_txn(
                txn=txn,
                table="tchap_red_list",
                keyvalues={"because_expired": True},
                retcols=["user_id"],
            )

            previously_expired_users = [row[0] for row in rows]

            # Among these users, figure out which ones are still expired.
            rows = DatabasePool.simple_select_many_txn(
                txn=txn,
                table="email_account_validity",
                column="user_id",
                iterable=previously_expired_users,
                keyvalues={},
                retcols=["user_id", "expiration_ts_ms"],
            )

            renewed_users: List[str] = []
            now_ms = int(time.time() * 1000)
            for row in rows:
                if row[1] > now_ms:
                    renewed_users.append(row[0])

            # Remove the users who aren't expired anymore.
            DatabasePool.simple_delete_many_txn(
                txn=txn,
                table="tchap_red_list",
                column="user_id",
                values=renewed_users,
                keyvalues={},
            )

            return renewed_users

        users_removed = await self._api.run_db_interaction(
            "tchap_red_list_remove_renewed_users",
            remove_renewed_users_txn,
        )
        for user in users_removed:
            await self._api.invalidate_cache(self._get_user_status, (user,))

        # Make the renewed users re-join the discovery room if there's one.
        for user in users_removed:
            await self._maybe_change_membership_in_discovery_room(user, "join")

    async def _setup_db(self) -> None:
        """Create the table needed to store the red list data.

        If the module is configured to interact with the email account validity module,
        also check that the table exists.
        """

        def setup_db_txn(txn: LoggingTransaction) -> None:
            sql = """
            CREATE TABLE IF NOT EXISTS tchap_red_list(
                user_id TEXT PRIMARY KEY,
                because_expired BOOLEAN NOT NULL DEFAULT FALSE
            );
            """
            txn.execute(sql, ())

            if self._config.use_email_account_validity:
                try:
                    txn.execute("SELECT * FROM email_account_validity LIMIT 0", ())
                except SynapseError:
                    raise ConfigError(
                        "use_email_account_validity is set but no email account validity"
                        " database table found."
                    )

        await self._api.run_db_interaction(
            "tchap_red_list_setup_db",
            setup_db_txn,
        )

    async def _add_to_red_list(
        self,
        user_id: str,
        because_expired: bool = False,
    ) -> None:
        """Add the given user to the red list.

        Args:
            user_id: the user to add to the red list.
            because_expired: whether the user is being added as a result of their
                account expiring.
        """

        def _add_to_red_list_txn(txn: LoggingTransaction) -> None:
            sql = """
            INSERT INTO tchap_red_list(user_id, because_expired) VALUES (?, ?)
            """
            txn.execute(sql, (user_id, because_expired))

        await self._api.run_db_interaction(
            "tchap_red_list_add",
            _add_to_red_list_txn,
        )
        await self._api.invalidate_cache(self._get_user_status, (user_id,))

        # If there is a room used for user discovery, make them leave it.
        await self._maybe_change_membership_in_discovery_room(user_id, "leave")

    async def _make_addition_permanent(self, user_id: str) -> None:
        """Update a user's addition to the red list to make it permanent so it's not
        removed automatically when the user renews their account.

        Args:
            user_id: the user to update.
        """

        def make_addition_permanent(txn: LoggingTransaction) -> None:
            DatabasePool.simple_update_one_txn(
                txn=txn,
                table="tchap_red_list",
                keyvalues={"user_id": user_id},
                updatevalues={"because_expired": False},
            )

        await self._api.run_db_interaction(
            "tchap_red_list_make_addition_permanent",
            make_addition_permanent,
        )
        await self._api.invalidate_cache(self._get_user_status, (user_id,))

    async def _remove_from_red_list(self, user_id: str) -> None:
        """Remove the given user from the red list.

        Args:
            user_id: the user to remove from the red list.
        """

        def _remove_from_red_list_txn(txn: LoggingTransaction) -> None:
            sql = """
            DELETE FROM tchap_red_list WHERE user_id = ?
            """
            txn.execute(sql, (user_id,))

        await self._api.run_db_interaction(
            "tchap_red_list_remove",
            _remove_from_red_list_txn,
        )

        await self._api.invalidate_cache(self._get_user_status, (user_id,))

        # If there is a room used for user discovery, make them join it.
        await self._maybe_change_membership_in_discovery_room(user_id, "join")

    @cached()
    async def _get_user_status(self, user_id: str) -> Tuple[bool, bool]:
        """Whether the given user is in the red list, and if so whether they have been
        added as a result of their account expiring.

        Args:
            user_id: the user to check.

        Returns:
            A tuple with the following values:
                * a boolean indicating whether the user is in the red list
                * a boolean indicating whether the user was added to the red list as a
                  result of their account expiring. Always False if the first value of
                  the tuple is False.
        """

        def _get_user_status_txn(txn: LoggingTransaction) -> Tuple[bool, bool]:
            row = DatabasePool.simple_select_one_txn(
                txn=txn,
                table="tchap_red_list",
                keyvalues={"user_id": user_id},
                retcols=["because_expired"],
                allow_none=True,
            )

            if row is None:
                return False, False

            return True, bool(row[0])

        return await self._api.run_db_interaction(
            "tchap_red_list_get_status",
            _get_user_status_txn,
        )

    async def _get_visible_users(self) -> Set[str]:
        """Selects active users who are not in the red list.

        Returns:
            A list of dictionaries, each with a user ID.
        """

        def select_users_txn(txn):
            txn.execute(
                """
                SELECT u.name
                FROM users u
                LEFT JOIN tchap_red_list trl ON u.name = trl.user_id
                WHERE u.deactivated = 0
                AND trl.user_id is NULL
                LIMIT 100
                """,
                (),
            )
            return txn.fetchall()

        visible_users: List[Dict[str, Union[str, int]]] = (
            await self._api.run_db_interaction("get_expired_users", select_users_txn)
        )
        return set(map(lambda user: user[0], visible_users))

    async def _update_discovery_room(self) -> None:
        if not self._config.discovery_room:
            return
        logger.info(
            "Add missing users to discovery room: %s", self._config.discovery_room
        )

        visible_users = await self._get_visible_users()
        logger.debug("Number of users on instance: %s", len(visible_users))
        joined_members_with_profile = (
            await self._state_storage_controller.get_users_in_room_with_profiles(
                self._config.discovery_room
            )
        )
        joined_members = joined_members_with_profile.keys()
        logger.debug("Number of users in discovery room: %s", len(joined_members))
        users_missing_in_room = set(visible_users).difference(set(joined_members))
        logger.debug(
            "Number of missing users in discovery room: %s", len(users_missing_in_room)
        )

        for index, user_id in enumerate(users_missing_in_room):
            await self._maybe_change_membership_in_discovery_room(user_id, "join")
            logger.info(
                "%s/%s Adding user %s in discovery room",
                index + 1,
                len(users_missing_in_room),
                user_id,
            )
        logger.info(
            "Add missing users to discovery room: %s is completed",
            self._config.discovery_room,
        )
