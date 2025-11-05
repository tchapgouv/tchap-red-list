# "Red list" module for Tchap

This module allows users to hide themselves from user search.

Users are expected to be in a single room, hidden from clients, to help user discovery
across a closed federation. When users update their global `im.vector.hide_profile`
account data with `{"hide_profile": True}`, they are removed from this discovery room,
and added to a local database table to filter them out from local results.

This module can also interact with the [synapse-email-account-validity](https://github.com/matrix-org/synapse-email-account-validity)
module. If this compatibility feature is enabled, the module will automatically scan for
expired and renewed users every hour. It will then add expired users to the red list and
remove renewed users from it (without updating the users' account data).

## Installation

From the virtual environment that you use for Synapse, install this module with:
```shell
pip install path/to/tchap-red-list
```
(If you run into issues, you may need to upgrade `pip` first, e.g. by running
`pip install --upgrade pip`)

Then alter your homeserver configuration, adding to your `modules` configuration:
```yaml
modules:
  - module: tchap_red_list.RedListManager
    config:
      # ID of the room used for user discovery.
      # Optional, defaults to no discovery_room.
      discovery_room:
        # Discovery room id that will be joined by users  
        active: "!discoroom3:example.com"
        # Discovery room id list containing the rest of the users (only leave can be performed)
        passives:
          - "!discoroom1:example.com"
          - "!discoroom2:example.com"
        # Send an alert email when `active_room_max_size` is reached
        active_room_max_size: 10000
        # Recipient of the alert email
        support_email: "support@example.com"
        # Whether to enable removing of red list users from discovery rooms (prevent consistency issue)
        # Could be high in performance  
        # Optional, defaults to false.  
        sync_red_list: false
      # Whether to enable compatibility with the synapse-email-account-validity module.
      # Optional, defaults to false.
      use_email_account_validity: false
      # Add user in discovery room by batch of `sync_user_batch_size`
      sync_user_batch_size: 1000
      # All background jobs will be executed every `job_interval_in_minutes` minutes
      job_interval_in_minutes: 60
```


## Development

In a virtual environment with pip ≥ 21.1, run
```shell
pip install -e .[dev]
```

To run the unit tests, you can either use:
```shell
tox -e py
```
or
```shell
trial tests
```

To run the linters and `mypy` type checker, use `./scripts-dev/lint.sh`.


## Releasing

The exact steps for releasing will vary; but this is an approach taken by the
Synapse developers (assuming a Unix-like shell):

 1. Set a shell variable to the version you are releasing (this just makes
    subsequent steps easier):
    ```shell
    version=X.Y.Z
    ```

 2. Update `setup.cfg` so that the `version` is correct.

 3. Stage the changed files and commit.
    ```shell
    git add -u
    git commit -m v$version -n
    ```

 4. Push your changes.
    ```shell
    git push
    ```

 5. When ready, create a signed tag for the release:
    ```shell
    git tag -s v$version
    ```
    Base the tag message on the changelog.

 6. Push the tag.
    ```shell
    git push origin tag v$version
    ```