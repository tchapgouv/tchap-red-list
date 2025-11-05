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
from typing import Optional, Tuple
from unittest.mock import AsyncMock, Mock, call

import aiounittest
from synapse.api.constants import Membership
from synapse.module_api import JsonDict

from tchap_red_list import ACCOUNT_DATA_TYPE, RedListManager
from tests import SQLiteStore, create_module, make_awaitable


class RedListTestCase(aiounittest.AsyncTestCase):
    user_id = "@alice:example"
    already_in_discovery_room_user = "@already_in_discovery_room_user:example"
    fake_user_1 = "@fake_user_1:example"
    fake_user_2 = "@fake_user_2:example"
    fake_user_3 = "@fake_user_3:example"

    def _setup_synapse_db(self, store: SQLiteStore) -> None:
        """Create a table mocking the one created by synapse-email-account-validity,
        except only with the columns used by the red list module, and populate it.

        Args:
            store: the store to use to create and populate the table.
        """
        txn = store.conn.cursor()

        txn.execute(
            "INSERT INTO users(name, deactivated) VALUES(?, ?)",
            (self.user_id, 0),
        )

        txn.execute(
            "INSERT INTO users(name, deactivated) VALUES(?, ?)",
            (self.already_in_discovery_room_user, 0),
        )

        store.conn.commit()

    async def test_other_data_type(self) -> None:
        """Tests that incoming account data with a different account data type than the
        one the module handles is ignored.
        """
        module, api, _ = await create_module()

        account_data_type = "org.matrix.foo"

        await module.update_red_list_status(
            user_id=self.user_id,
            room_id=None,
            account_data_type=account_data_type,
            content={"foo": "bar"},
        )

        api.run_db_interaction.assert_not_called()
        api.update_room_membership.assert_not_called()

    async def test_no_hide_profile(self) -> None:
        """Tests that account data of the right type but with a content that doesn't
        include the hide_profile property is considered as if it was present and equal to
        False.
        """
        module, api = await self._setup_user_in_list()

        # Invalidate the cache, so it doesn't interfere with the call counts.
        module._get_user_status.invalidate((self.user_id,))

        # Trigger the callback with an account data that's missing the hide_profile key
        # in its content.
        await module.update_red_list_status(
            user_id=self.user_id,
            room_id=None,
            account_data_type=ACCOUNT_DATA_TYPE,
            content={},
        )

        # There should be two database calls: one to check the user's status, and one to
        # update it.
        self.assertEqual(
            api.run_db_interaction.call_count, 2, api.run_db_interaction.mock_calls
        )
        api.update_room_membership.assert_not_called()

        # Check that the user is not in the list anymore.
        in_list, _ = await module._get_user_status(self.user_id)
        self.assertFalse(in_list)

    async def test_status_cached(self) -> None:
        """Tests that users statuses are correctly cached and invalidated."""
        module, api, _ = await create_module()

        # Get the user's status and check we made a database call.
        in_list, _ = await module._get_user_status(self.user_id)
        self.assertFalse(in_list)
        api.run_db_interaction.assert_called_once()

        # Get the user's status again and check we didn't make an additional database
        # call.
        in_list, _ = await module._get_user_status(self.user_id)
        self.assertFalse(in_list)
        api.run_db_interaction.assert_called_once()

        # Add the user to the list and get their status again to check that it
        # invalidated the cache (and caused another database call).
        await module.update_red_list_status(
            user_id=self.user_id,
            room_id=None,
            account_data_type=ACCOUNT_DATA_TYPE,
            content={"hide_profile": True},
        )
        # Reset the mock so that the database call from update_red_list_status doesn't
        # interfere.
        api.run_db_interaction.reset_mock()
        in_list, _ = await module._get_user_status(self.user_id)
        self.assertTrue(in_list)
        api.run_db_interaction.assert_called_once()

        # Remove the user from the list and check the same thing.
        await module.update_red_list_status(
            user_id=self.user_id,
            room_id=None,
            account_data_type=ACCOUNT_DATA_TYPE,
            content={"hide_profile": False},
        )
        # Reset the mock so that the database call from update_red_list_status doesn't
        # interfere.
        api.run_db_interaction.reset_mock()
        in_list, _ = await module._get_user_status(self.user_id)
        self.assertFalse(in_list)
        api.run_db_interaction.assert_called_once()

    async def test_add_to_list_no_discovery(self) -> None:
        """Tests adding a user to the red list (with no discovery room)"""
        module, api, _ = await create_module()

        await module.update_red_list_status(
            user_id=self.user_id,
            room_id=None,
            account_data_type=ACCOUNT_DATA_TYPE,
            content={"hide_profile": True},
        )

        self.assertEqual(
            api.run_db_interaction.call_count, 2, api.run_db_interaction.mock_calls
        )
        api.update_room_membership.assert_not_called()

        in_list, _ = await module._get_user_status(self.user_id)

        self.assertTrue(in_list)

    async def test_remove_from_list_no_discovery(self) -> None:
        """Tests removing a user from the red list (with no discovery room)"""
        module, api = await self._setup_user_in_list()

        await module.update_red_list_status(
            user_id=self.user_id,
            room_id=None,
            account_data_type=ACCOUNT_DATA_TYPE,
            content={"hide_profile": False},
        )

        self.assertEqual(
            api.run_db_interaction.call_count, 2, api.run_db_interaction.mock_calls
        )
        api.update_room_membership.assert_not_called()

        in_list, _ = await module._get_user_status(self.user_id)

        self.assertFalse(in_list)

    async def test_add_to_list_with_one_discovery(self) -> None:
        """Tests adding a user to the red list (with a discovery room)"""
        room_id = "!someroom:test"
        module, api, _ = await create_module({"discovery_room": {"active": room_id}})
        api._hs.get_room_member_handler().store.check_local_user_in_room = AsyncMock(
            side_effect=lambda i_user_id, i_room_id: True
        )

        await module.update_red_list_status(
            user_id=self.user_id,
            room_id=None,
            account_data_type=ACCOUNT_DATA_TYPE,
            content={"hide_profile": True},
        )

        self.assertEqual(
            api.run_db_interaction.call_count, 2, api.run_db_interaction.mock_calls
        )
        api.update_room_membership.assert_called_once_with(
            sender=self.user_id,
            target=self.user_id,
            room_id=room_id,
            new_membership=Membership.LEAVE,
        )

        in_list, _ = await module._get_user_status(self.user_id)
        self.assertTrue(in_list)

    async def test_add_to_list_with_multi_discovery(self) -> None:
        """Tests adding a user to the red list (with multiple discovery rooms)"""
        room_id1 = "!someroom1:test"
        room_id2 = "!someroom2:test"
        room_id3 = "!someroom3:test"
        module, api, store = await create_module(
            {"discovery_room": {"active": room_id1, "passives": [room_id2, room_id3]}}
        )
        room_results = {
            room_id1: True,
            room_id2: False,
            room_id3: True,
        }
        api._hs.get_room_member_handler().store.check_local_user_in_room = AsyncMock(
            side_effect=lambda user_id, room_id: room_results.get(room_id, False)
        )

        await module.update_red_list_status(
            user_id=self.user_id,
            room_id=None,
            account_data_type=ACCOUNT_DATA_TYPE,
            content={"hide_profile": True},
        )

        self.assertEqual(
            api.run_db_interaction.call_count, 2, api.run_db_interaction.mock_calls
        )
        expected_calls = [
            call(
                sender=self.user_id,
                target=self.user_id,
                room_id=room_id1,
                new_membership=Membership.LEAVE,
            ),
            call(
                sender=self.user_id,
                target=self.user_id,
                room_id=room_id3,
                new_membership=Membership.LEAVE,
            ),
        ]
        api.update_room_membership.assert_has_calls(expected_calls, any_order=True)

        in_list, _ = await module._get_user_status(self.user_id)
        self.assertTrue(in_list)

    async def test_remove_from_list_with_one_discovery(self) -> None:
        """Tests removing a user from the red list (with a discovery room)"""
        room_id = "!someroom:test"
        module, api = await self._setup_user_in_list(
            {"discovery_room": {"active": room_id}}
        )
        api._hs.get_room_member_handler().store.check_local_user_in_room = AsyncMock(
            side_effect=lambda i_user_id, i_room_id: True
        )

        await module.update_red_list_status(
            user_id=self.user_id,
            room_id=None,
            account_data_type=ACCOUNT_DATA_TYPE,
            content={"hide_profile": False},
        )

        self.assertEqual(
            api.run_db_interaction.call_count, 2, api.run_db_interaction.mock_calls
        )
        api.update_room_membership.assert_called_once_with(
            sender=self.user_id,
            target=self.user_id,
            room_id=room_id,
            new_membership=Membership.JOIN,
        )

        in_list, _ = await module._get_user_status(self.user_id)
        self.assertFalse(in_list)

    async def test_remove_from_list_with_multi_discovery(self) -> None:
        """Tests removing a user from the red list (with multiple discovery rooms)"""
        room_id1 = "!someroom1:test"
        room_id2 = "!someroom2:test"
        room_id3 = "!someroom3:test"

        module, api = await self._setup_user_in_list(
            {"discovery_room": {"active": room_id1, "passives": [room_id2, room_id3]}}
        )
        room_results = {
            room_id1: True,
            room_id2: False,
            room_id3: True,
        }
        api._hs.get_room_member_handler().store.check_local_user_in_room = AsyncMock(
            side_effect=lambda user_id, room_id: room_results.get(room_id, False)
        )

        await module.update_red_list_status(
            user_id=self.user_id,
            room_id=None,
            account_data_type=ACCOUNT_DATA_TYPE,
            content={"hide_profile": False},
        )

        self.assertEqual(
            api.run_db_interaction.call_count, 2, api.run_db_interaction.mock_calls
        )
        api.update_room_membership.assert_called_once_with(
            sender=self.user_id,
            target=self.user_id,
            room_id=room_id1,
            new_membership=Membership.JOIN,
        )

        in_list, _ = await module._get_user_status(self.user_id)
        self.assertFalse(in_list)

    async def test_update_with_one_discovery_room(self) -> None:
        """Tests update the discovery room by adding `user_id` when `user_id` is not in the active discovery room"""
        room_id = "!someroom:test"
        module, api, store = await create_module(
            {"discovery_room": {"active": room_id}}
        )
        api._hs.get_storage_controllers().state.get_users_in_room_with_profiles.return_value = make_awaitable(
            {self.already_in_discovery_room_user: ()}
        )
        self._setup_synapse_db(store)

        await module._update_discovery_room_with_red_list()

        self.assertEqual(
            api.run_db_interaction.call_count, 1, api.run_db_interaction.mock_calls
        )
        api.update_room_membership.assert_called_once_with(
            sender=self.user_id,
            target=self.user_id,
            room_id=room_id,
            new_membership=Membership.JOIN,
        )

        in_list, _ = await module._get_user_status(self.user_id)
        self.assertFalse(in_list)

    async def test_update_with_multi_discovery_room(self) -> None:
        """Tests update the discovery room by adding `user_id` when `user_id` is not in any in discovery room"""
        room_id1 = "!someroom1:test"
        room_id2 = "!someroom2:test"
        room_id3 = "!someroom3:test"
        room_results = {
            room_id1: {},
            room_id2: {},
            room_id3: {self.already_in_discovery_room_user: ()},
        }
        module, api, store = await create_module(
            {"discovery_room": {"active": room_id1, "passives": [room_id2, room_id3]}}
        )
        api._hs.get_storage_controllers().state.get_users_in_room_with_profiles = (
            AsyncMock(side_effect=lambda room_id: room_results.get(room_id, {}))
        )
        self._setup_synapse_db(store)

        await module._update_discovery_room_with_red_list()

        self.assertEqual(
            api.run_db_interaction.call_count, 1, api.run_db_interaction.mock_calls
        )
        api.update_room_membership.assert_called_once_with(
            sender=self.user_id,
            target=self.user_id,
            room_id=room_id1,
            new_membership=Membership.JOIN,
        )
        in_list, _ = await module._get_user_status(self.user_id)
        self.assertFalse(in_list)

    async def test_no_update_with_multi_discovery_room(self) -> None:
        """Tests no update of the discovery room when `user_id` is already in discovery room `room_id3`"""
        room_id1 = "!someroom1:test"
        room_id2 = "!someroom2:test"
        room_id3 = "!someroom3:test"
        room_results = {
            room_id1: {self.user_id: ()},
            room_id2: {},
            room_id3: {self.already_in_discovery_room_user: ()},
        }
        module, api, store = await create_module(
            {"discovery_room": {"active": room_id1, "passives": [room_id2, room_id3]}}
        )
        api._hs.get_storage_controllers().state.get_users_in_room_with_profiles = (
            AsyncMock(side_effect=lambda room_id: room_results.get(room_id, {}))
        )
        self._setup_synapse_db(store)

        await module._update_discovery_room_with_red_list()

        self.assertEqual(
            api.run_db_interaction.call_count, 1, api.run_db_interaction.mock_calls
        )
        api.update_room_membership.assert_not_called()
        in_list, _ = await module._get_user_status(self.user_id)
        self.assertFalse(in_list)

    async def test_send_email(self):
        room_id1 = "!someroom1:test"
        room_id2 = "!someroom2:test"
        room_id3 = "!someroom3:test"
        room_results = {
            room_id1: {
                self.fake_user_1: (),
                self.fake_user_2: (),
                self.fake_user_3: (),
            },
            room_id2: {},
            room_id3: {},
        }
        module, api, store = await create_module(
            {
                "discovery_room": {
                    "active": room_id1,
                    "passives": [room_id2, room_id3],
                    "support_email": "support@homeserver",
                    "active_room_max_size": 3,
                },
            }
        )
        api._hs.get_storage_controllers().state.get_users_in_room_with_profiles = (
            AsyncMock(side_effect=lambda room_id: room_results.get(room_id, {}))
        )

        await module._update_discovery_room_with_red_list()

        self.assertEqual(module._api.send_mail.call_count, 1)
        _, kwargs = module._api.send_mail.call_args
        self.assertNotEqual(kwargs["html"].find(room_id1), -1)
        self.assertNotEqual(kwargs["text"].find(room_id1), -1)

    async def _setup_user_in_list(
        self, config: Optional[JsonDict] = None
    ) -> Tuple[RedListManager, Mock]:
        """Performs the initial setup for tests that require a user to already be present
        in the red list.

        Args:
            config: a config to pass onto create_module.

        Returns:
            The return values from create_module.
        """
        module, api, _ = await create_module(config)
        api._hs.get_room_member_handler().store.check_local_user_in_room = AsyncMock(
            side_effect=lambda i_user_id, i_room_id: self.user_id == i_user_id
        )
        await module._add_to_red_list(self.user_id)
        # Reset the mocks, so the action we just performed doesn't interfere with the
        # call counts.
        api.run_db_interaction.reset_mock()
        api.update_room_membership.reset_mock()
        return module, api
