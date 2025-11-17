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
import os
import time
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set, Tuple, Union

import attr
from pkg_resources import DistributionNotFound, get_distribution
from synapse.api.constants import Membership
from synapse.api.errors import LimitExceededError
from synapse.module_api import (
    DatabasePool,
    JsonDict,
    ModuleApi,
    P,
    T,
    UserProfile,
    cached,
)
from synapse.module_api.errors import ConfigError, SynapseError
from synapse.storage.database import LoggingTransaction
from typing_extensions import Concatenate

UPDATE_MEMBERSHIP_MAX_RETRY = 11

logger = logging.getLogger(__name__)

ACCOUNT_DATA_TYPE = "im.vector.hide_profile"

try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    # package is not installed
    pass


@attr.s(auto_attribs=True, frozen=True)
class RedListManagerDiscoveryRoomConfig:
    active: Optional[str] = None
    passives: List[str] = []
    support_email: Optional[str] = None
    active_room_max_size: int = 10000
    sync_red_list: bool = False

    def all(self) -> List[str]:
        return [self.active] + self.passives

    def __attrs_post_init__(self):
        if not self.active:
            raise ConfigError(
                "discovery_room is set but discovery_room.active is not configured"
            )


@attr.s(auto_attribs=True, frozen=True)
class RedListManagerConfig:
    discovery_room: Optional[RedListManagerDiscoveryRoomConfig] = None
    use_email_account_validity: bool = False
    sync_user_batch_size: int = 100
    job_interval_in_minutes: int = 60

    def is_discovery_room_feature_enabled(self) -> bool:
        return self.discovery_room is not None


class RedListManager:
    def __init__(
        self, config: RedListManagerConfig, api: ModuleApi, setup_db: bool = True
    ):
        # Keep a reference to the config and Module API
        self._api = api
        self.server_name = self._api.server_name
        self._config = config
        self._state_storage_controller_state = (
            self._api._hs.get_storage_controllers().state
        )
        self._room_member_handler_store = self._api._hs.get_room_member_handler().store
        self._clock = self._api._hs.get_clock()
        (self._template_html, self._template_text,) = self._api.read_templates(
            ["discovery_room_alert.html", "discovery_room_alert.txt"],
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates"),
        )

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
            self._api.run_as_background_process(__name__ + ":_setup_db", self._setup_db)

        # self._api.looping_background_call is taking too much time the next call is not scheduled
        # https://github.com/element-hq/synapse/blob/ec885ffd334df29c99aaf722424d61a9e7739a1a/synapse/util/__init__.py#L130-L130
        if self._config.use_email_account_validity:
            self._api.looping_background_call(
                self._add_expired_users,
                self._config.job_interval_in_minutes * 60 * 1000,
            )
            self._api.looping_background_call(
                self._remove_renewed_users,
                self._config.job_interval_in_minutes * 60 * 1000,
            )
            if self._config.is_discovery_room_feature_enabled():
                self._api.looping_background_call(
                    self._update_discovery_room_with_red_list_and_email_account_validity,
                    self._config.job_interval_in_minutes * 60 * 1000,
                )
        else:
            if self._config.is_discovery_room_feature_enabled():
                self._api.looping_background_call(
                    self._update_discovery_room_with_red_list,
                    self._config.job_interval_in_minutes * 60 * 1000,
                )

    @staticmethod
    def parse_config(config: Dict[str, Any]) -> RedListManagerConfig:
        discovery_room = config.get("discovery_room")

        if discovery_room:
            if isinstance(discovery_room, dict):
                discovery_room = RedListManagerDiscoveryRoomConfig(**discovery_room)
            else:
                raise ConfigError(
                    "discovery_room is set but discovery_room.active is not configured"
                )

        return RedListManagerConfig(
            discovery_room=discovery_room,
            use_email_account_validity=config.get("use_email_account_validity", False),
            sync_user_batch_size=config.get("sync_user_batch_size", 100),
            job_interval_in_minutes=config.get("job_interval_in_minutes", 60)
        )

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
        if account_data_type != ACCOUNT_DATA_TYPE:
            return
        logger.debug(f"Update Red List {user_id}={content}")

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

    async def _change_membership_in_discovery_room(
        self, user_id: str, membership: str
    ) -> None:
        """Change a user's membership in the discovery room.

        Does nothing if no discover room has been configured.

        Args:
            user_id: the user to change the membership of.
            membership: the membership to set for this user.
                - JOIN only on the active discovery room
                - LEAVE on all discovery room if user is present
        """
        if not self._config.is_discovery_room_feature_enabled():
            return

        for retry_nb in range(1, UPDATE_MEMBERSHIP_MAX_RETRY):
            try:
                # Performs join only on the active discovery room
                if membership == Membership.JOIN:
                    await self._api.update_room_membership(
                        sender=user_id,
                        target=user_id,
                        room_id=self._config.discovery_room.active,
                        new_membership=membership,
                    )
                    logger.debug(
                        "User [%s] joined Active Discovery Room: %s",
                        user_id,
                        self._config.discovery_room.active,
                    )
                # Performs leave on all discovery room if user is present
                elif membership == Membership.LEAVE:
                    for room_id in self._config.discovery_room.all():
                        is_user_in_room = await self._room_member_handler_store.check_local_user_in_room(
                            user_id, room_id
                        )
                        if is_user_in_room:
                            await self._api.update_room_membership(
                                sender=user_id,
                                target=user_id,
                                room_id=room_id,
                                new_membership=membership,
                            )
                            logger.debug(
                                "User [%s] left Discovery Room: %s", user_id, room_id
                            )
                break
            except LimitExceededError:
                logger.warning(
                    "Update discovery room : %s - %s - %s - RateLimit has been reached",
                    user_id,
                    membership,
                    retry_nb,
                )
                await self._clock.sleep(0.5 * retry_nb)
            except RuntimeError as e:
                logger.warning(
                    "Cannot update discovery room : %s - %s : %s",
                    user_id,
                    membership,
                    e,
                )
                break

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
            # Retrieve all the expired users and not in the red list.
            sql = """
            SELECT eav.user_id
            FROM email_account_validity eav
            LEFT JOIN tchap_red_list trl ON eav.user_id = trl.user_id
            WHERE eav.expiration_ts_ms <= ?
            AND trl.user_id is NULL
            LIMIT 100
            """

            now_ms = int(time.time() * 1000)
            txn.execute(sql, (now_ms,))
            expired_users_rows = txn.fetchall()

            expired_users_not_in_red_list = [row[0] for row in expired_users_rows]

            # Add all the expired users not in the red list.
            sql = """
            INSERT INTO tchap_red_list(user_id, because_expired) VALUES(?, ?)
            """
            for user in expired_users_not_in_red_list:
                txn.execute(sql, (user, True))
                logger.debug("Add expired user %s to red list", user)

            return expired_users_not_in_red_list

        logger.info("Add expired users to red list")

        users_added = await self._api.run_db_interaction(
            "tchap_red_list_hide_expired_users",
            add_expired_users_txn,
        )

        # Make the expired users leave the discovery room if there's one.
        for user in users_added:
            await self._api.invalidate_cache(self._get_user_status, (user,))
            await self._change_membership_in_discovery_room(user, Membership.LEAVE)
        logger.info(
            "Add expired users to red list is completed : %s have been added",
            len(users_added),
        )

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
            await self._change_membership_in_discovery_room(user, Membership.JOIN)
            logger.debug("Add renewed user %s to discovery room", user)

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
        await self._change_membership_in_discovery_room(user_id, Membership.LEAVE)
        logger.debug("Add user %s to red list", user_id)

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
        await self._change_membership_in_discovery_room(user_id, Membership.JOIN)
        logger.debug("Remove user %s from red list", user_id)

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

    async def _select_users(
        self, desc: str, select_users: Callable[Concatenate[LoggingTransaction, P], T]
    ) -> Set[str]:
        """Selects users.

        Returns:
            A list of dictionaries, each with a user ID.
        """

        users: List[Dict[str, Union[str, int]]] = await self._api.run_db_interaction(
            desc, select_users
        )
        return set(map(lambda user: user[0], users))

    async def _get_visible_users_not_in_red_list(self) -> Set[str]:
        """Selects active users who are not in the red list.

        Returns:
            A list of dictionaries, each with a user ID.
        """

        def select_users_not_in_red_list_txn(txn):
            txn.execute(
                """
                SELECT u.name
                FROM users u
                LEFT JOIN tchap_red_list trl ON u.name = trl.user_id
                WHERE u.deactivated = 0
                AND trl.user_id is NULL
                ORDER BY u.creation_ts DESC
                """,
                (),
            )
            return txn.fetchall()

        return await self._select_users(
            "get_visible_users_not_in_red_list", select_users_not_in_red_list_txn
        )

    async def _get_users_in_red_list(self) -> Set[str]:
        """Selects active users who are in the red list.

        Returns:
            A list of dictionaries, each with a user ID.
        """

        def select_users_in_red_list_txn(txn):
            txn.execute(
                """
                SELECT trl.user_id
                FROM tchap_red_list trl
                LEFT JOIN users u ON u.name = trl.user_id
                WHERE u.deactivated = 0
                ORDER BY u.creation_ts DESC
                """,
                (),
            )
            return txn.fetchall()

        return await self._select_users(
            "_get_users_in_red_list", select_users_in_red_list_txn
        )

    async def _get_visible_users_not_expired_not_in_red_list(self) -> Set[str]:
        """Selects active users who are not in the red list and not expired.

        Returns:
            A list of dictionaries, each with a user ID.
        """

        def select_users_not_expired_not_in_red_list_txn(txn):
            now_ms = int(time.time() * 1000)
            txn.execute(
                """
                SELECT u.name
                FROM users u
                LEFT JOIN tchap_red_list trl ON u.name = trl.user_id
                LEFT JOIN email_account_validity eav ON u.name = eav.user_id
                WHERE u.deactivated = 0
                AND trl.user_id is NULL
                AND (eav.expiration_ts_ms > ? OR eav.user_id is NULL)
                ORDER BY u.creation_ts DESC
                """,
                (now_ms,),
            )
            return txn.fetchall()

        return await self._select_users(
            "get_visible_users_not_expired_not_in_red_list",
            select_users_not_expired_not_in_red_list_txn,
        )

    async def _update_discovery_room_with_red_list(self) -> None:
        if not self._config.is_discovery_room_feature_enabled():
            return
        logger.info(
            "Add missing users to discovery room: %s",
            self._config.discovery_room.active,
        )
        # Get all visible users (not on red list)
        await self._update_discovery_room(self._get_visible_users_not_in_red_list)

    async def _update_discovery_room_with_red_list_and_email_account_validity(
        self,
    ) -> None:
        if (
            not self._config.is_discovery_room_feature_enabled()
            or not self._config.use_email_account_validity
        ):
            return
        logger.info(
            "Add missing users to discovery room: %s",
            self._config.discovery_room.active,
        )
        # Get all visible users (not on red list and not expired users)
        await self._update_discovery_room(
            self._get_visible_users_not_expired_not_in_red_list
        )

    async def _update_discovery_room(
        self, get_visible_users_fn: Callable[[], Awaitable[Set[str]]]
    ) -> None:
        # Synchronize Red List in case we have an issue
        if self._config.discovery_room.sync_red_list:
            users_in_red_list = await self._get_users_in_red_list()
            number_users_in_red_list = len(users_in_red_list)
            logger.info(
                "Synchronize Red List: Number of user on red list that will Leave from all discovery rooms: %s",
                number_users_in_red_list,
            )
            for index, user_id in enumerate(users_in_red_list):
                await self._change_membership_in_discovery_room(
                    user_id, Membership.LEAVE
                )
                logger.debug(
                    "Synchronize Red List - [%s/%s users] - %s left all discovery rooms",
                    index + 1,
                    number_users_in_red_list,
                    user_id,
                )
        # Get visible users
        visible_users = await get_visible_users_fn()
        logger.debug("Number of users on homeserver: %s", len(visible_users))
        # Get all users from all discovery rooms
        users_missing_in_room = set(visible_users)
        all_discovery_rooms = self._config.discovery_room.all()
        number_of_discovery_rooms = len(all_discovery_rooms)
        for index, discovery_room_id in enumerate(all_discovery_rooms):
            joined_members_with_profile = await self._state_storage_controller_state.get_users_in_room_with_profiles(
                discovery_room_id
            )
            joined_members = joined_members_with_profile.keys()
            number_of_joined_members = len(joined_members)
            logger.debug(
                "Number of users in discovery room %s/%s [%s]: %s",
                index + 1,
                number_of_discovery_rooms,
                discovery_room_id,
                number_of_joined_members,
            )
            # Send email if active is room has reached limit in order to create other room
            if (
                self._config.discovery_room.support_email
                and discovery_room_id == self._config.discovery_room.active
                and number_of_joined_members
                >= self._config.discovery_room.active_room_max_size
            ):
                template_vars = {
                    "active_room_max_size": self._config.discovery_room.active_room_max_size,
                    "active_room_id": self._config.discovery_room.active,
                    "number_of_joined_members": number_of_joined_members,
                }

                html_text = self._template_html.render(**template_vars)
                plain_text = self._template_text.render(**template_vars)
                await self._api.send_mail(
                    recipient=self._config.discovery_room.support_email,
                    subject=f"{self.server_name} - Discovery Room has reached limit",
                    html=html_text,
                    text=plain_text,
                )
                logger.debug(
                    "Send an alert email to %s [quota=%s, active_discovery_room=%s, number_of_joined_members=%s]",
                    self._config.discovery_room.support_email,
                    self._config.discovery_room.active_room_max_size,
                    self._config.discovery_room.active,
                    number_of_joined_members,
                )

            users_missing_in_room = users_missing_in_room.difference(
                set(joined_members)
            )
            logger.debug(
                "Current number of missing users after checking discovery rooms %s/%s [%s]: %s",
                index + 1,
                number_of_discovery_rooms,
                discovery_room_id,
                len(users_missing_in_room),
            )

        logger.info(
            "Number of missing users in all discovery rooms: %s",
            len(users_missing_in_room),
        )
        users_missing_in_room_batch = list(users_missing_in_room)[
            : self._config.sync_user_batch_size
        ]
        for index, user_id in enumerate(users_missing_in_room_batch):
            await self._change_membership_in_discovery_room(user_id, Membership.JOIN)
            logger.info(
                "%s/%s Adding user %s in discovery room",
                index + 1,
                len(users_missing_in_room_batch),
                user_id,
            )
        logger.info(
            "Add missing users to discovery room: %s is completed",
            self._config.discovery_room.active,
        )
