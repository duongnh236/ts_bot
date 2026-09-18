# Android recovery v123

Network loss without a server disconnect-cause packet retries using the existing backoff.
An explicit server kick with a nonzero cause turns off only that account, including causes
30, 47 and 90. Cause 60 turns off all accounts. User logout never reconnects.

During Android train, a disconnected member is excluded temporarily from the route's
party/map/city barrier. Healthy accounts continue towards the selected farm point.
The returning member runs its own Ground route to the farm map, joins the leader's
live channel, and walks to a known safe point closest to the leader. It opens the
invitation gate there. Leader brings its current party to that safe, invites, waits for
the server roster to include the returning member, then goes back to the saved farm point.
Without known safe data the member waits and reports the reason instead of guessing.

If leader loses its connection, online members return to the city nearest the selected
farm map and wait. Once leader is online and those members have confirmed city arrival,
the normal train rally/party/Ground route command starts again.

Preparation means city teleport, channel synchronization and waiting for the initial
party. A network-lost member is allowed to catch up independently; leader loss interrupts
the preparation waits so members can return to town.

Character and pet death-return preferences are independent per-account settings shown
in the character information panel. They persist in Android account settings, restore
before game connection and update the game's machinebox flags when safe outside combat.
