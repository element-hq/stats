#!/usr/bin/env python3

import argparse
import datetime
import logging
import os
import time
from collections import Counter, defaultdict
from typing import (Collection, Dict, Iterable, List, Mapping, Optional,
                    Sequence, Set, Tuple)

import attr

import MySQLdb
from psycopg2 import connect

# Script to calculate user retention cohorts and output the results,
# comma separated to stdout
from psycopg2.extensions import make_dsn

ELEMENT_ELECTRON = "electron"
WEB = "web"
ELEMENT_ANDROID = "android"
RIOTX_ANDROID = "android-riotx"
ELEMENT_IOS = "ios"
MISSING = "missing"
OTHER = "other"

MS_PER_DAY = 24 * 60 * 60 * 1000

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)


class Config:
    def __init__(self):
        self.pg_dsn = make_dsn(
            host=os.environ["SYNAPSE_DB_HOST"],
            # port is optional and make_dsn ignores None values, so we use .get()
            port=os.environ.get("SYNAPSE_DB_PORT"),
            user=os.environ["SYNAPSE_DB_USERNAME"],
            password=os.environ["SYNAPSE_DB_PASSWORD"],
            dbname=os.environ["SYNAPSE_DB_DATABASE"],
            options=os.environ["SYNAPSE_DB_OPTIONS"],
        )
        self.STATS_DB_HOST = os.environ["STATS_DB_HOST"]
        self.STATS_DB_USERNAME = os.environ["STATS_DB_USERNAME"]
        self.STATS_DB_PASSWORD = os.environ["STATS_DB_PASSWORD"]
        self.STATS_DB_DATABASE = os.environ["STATS_DB_DATABASE"]

    def get_conn(self):
        conn = connect(self.pg_dsn)
        conn.set_session(readonly=True, autocommit=True)
        return conn


def str_to_ts(datestring):
    return int(
        1000 * time.mktime(datetime.datetime.strptime(datestring, "%Y-%m-%d").timetuple())
    )


def ts_to_str(ts):
    """Converts unix timestamp date string
    Args:
        ts (int): unix timestamp in milliseconds
    Returns:
        (str): date in format %Y-%m-%d
    """

    return(datetime.datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d"))


def get_args():
    ap = argparse.ArgumentParser()

    ap.add_argument(
        "-p",
        "--period",
        type=int,
        choices=[1, 7, 30],
        default=7,
        help="""
            Period over which cohorts and buckets are calculated, in days. Defaults to
            %(default)s.
            """,
    )
    ap.add_argument(
        "-b",
        "--buckets",
        type=int,
        default=6,
        help="Number of buckets/cohorts to analyze. Defaults to %(default)s.",
    )

    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Whether to print out the MySQL statements rather than actually executing them."
    )

    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--cohort_start_date",
        type=lambda d: datetime.datetime.strptime(d, "%Y-%m-%d"),
        help="""
            Enable cohort mode. In this mode, a single cohort of PERIOD days is tracked
            through BUCKETS activity buckets, each of PERIOD days. Option gives the
            first day of the cohort to be tracked.
            """,
    )

    group.add_argument(
        "--bucket_start_date",
        type=lambda d: datetime.datetime.strptime(d, "%Y-%m-%d"),
        help="""
            Enable bucket mode. In this mode, a single activity bucket of PERIOD days
            is analyzed for activity from each of BUCKETS cohorts of PERIOD days.
            Option gives the first day of the bucket to be analysed.
            """,
    )

    return ap.parse_args()


def parse_cohort_parameters(args):
    if args.cohort_start_date:
        mode = "cohort"
        date = args.cohort_start_date
    elif args.bucket_start_date:
        mode = "bucket"
        date = args.bucket_start_date
    date = int(date.strftime("%s")) * 1000

    period = args.period
    if period == 1:
        table = 'cohorts_daily'
    elif period == 7:
        table = 'cohorts_weekly'
    elif period == 30:
        table = 'cohorts_monthly'
    else:
        raise ValueError(f"Unexpected period {period}")
    period = int(period) * MS_PER_DAY

    now = int(time.time()) * 1000
    if (date + period) > now:
        raise ValueError(f"{date} is too recent, 0 periods will fit between it and now")

    return mode, date, args.buckets, period, table, args.dry_run


def user_agent_to_client(user_agent):
    if user_agent is None or len(user_agent) == 0:
        return MISSING

    ua = user_agent.lower()
    if "riot" in ua or "element" in ua:
        if "electron" in ua:
            return ELEMENT_ELECTRON
        elif "android" in ua and "riotx" in ua:
            return RIOTX_ANDROID
        elif "android" in ua:
            return ELEMENT_ANDROID
        elif "ios" in ua:
            return ELEMENT_IOS
    elif "mozilla" in ua or "gecko" in ua:
        return WEB
    elif "synapse" in ua or "okhttp" in ua or "python-requests" in ua:
        # Never consider this for over-writing of any other client type
        return MISSING

    return OTHER


@attr.s(frozen=True, slots=True)
class User:
    """Information about a specific user in the cohort"""
    user_id = attr.ib(type=str)

    # a list of the SSO auth_providers that this user can use
    auth_providers = attr.ib(type=Collection[str])


def get_new_users(start: int, stop: int) -> Sequence[User]:
    """Get a list of all users that registered an account during
    the given timeframe

    Args:
        start: start of the timeframe to check, inclusive, as ms since the
            epoch.

        stop: end of the timeframe to check, exclusive, as ms since the
            epoch.
    Returns:
        A list of distinct users
    """

    new_user_sql = """
        SELECT users.name, uei.auth_provider
        FROM users
        LEFT JOIN user_external_ids AS uei ON uei.user_id=users.name
        WHERE appservice_id is NULL
            AND is_guest = 0
            AND creation_ts >= %(start_date_seconds)s
            AND creation_ts < %(end_date_seconds)s
        """

    # for each user_id, a list of auth providers
    users = defaultdict(list)  # type: Dict[str, List[str]]

    begin = time.time()
    with CONFIG.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                new_user_sql,
                {
                    "start_date_seconds": start / 1000,
                    "end_date_seconds": stop / 1000,
                },
            )
            for user, ap in cursor:
                aps = users[user]
                if ap:
                    aps.append(ap)
    conn.close()

    # Running this query on secondary database not tuned for long running queries
    # this is really heavy handed way of allowing background processes to keep up :/
    pause = time.time() - begin
    time.sleep(pause)
    return [User(user_id, aps) for user_id, aps in users.items()]


def get_bucket_devices_by_user(
    users: Collection[str], start: int, stop: int
) -> Dict[str, Collection[str]]:
    """Given a list of users, get the device IDs that they used in the given period
    """
    if len(users) == 0:
        return {}

    cohort_sql = """SELECT DISTINCT user_id, device_id
                        FROM user_daily_visits
                        WHERE user_id IN %s
                        AND timestamp >= %s and timestamp < %s
                        AND device_id IS NOT NULL
                    """

    res = defaultdict(list)
    begin = time.time()
    with CONFIG.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                cohort_sql,
                (tuple(users), int(start), int(stop)),
            )
            for user_id, device_id in cursor:
                res[user_id].append(device_id)
    conn.close()

    # Running this query on secondary database not tuned for long running queries
    # this is really heavy handed way of allowing background processes to keep up :/
    pause = time.time() - begin
    time.sleep(pause)
    return res


def get_user_agents(
        users: Collection[str], start: int
) -> Sequence[Tuple[str, str, Optional[str]]]:
    """
    Given a list of user ids, find the user agents of all the devices that they have
    used to access the server since the given timestamp

    Returns:
        a list of (user_id, device_id, user_agent) triplets. Note that it may contain
            duplicates as well as null user agents!
    """
    if len(users) == 0:
        return []

    # First we fetch a list of devices that the users have used since the given
    # timestamp, and we then join that to both the `user_ips` and the `devices` tables
    # to look for useragents that have come from that device.
    #
    # XXX: why do we check both tables? `devices` will only include one user-agent, but
    #    why do we need to check it at all? Possibly because `user_ips` only includes
    #    28 days's worth of data so we fall back to `devices` as an approximiation for
    #    earlier traffic (which should only matter when we're trying to back-populate
    #    very old cohort usage data?)
    #
    cohort_sql = """WITH user_devices as (
                        SELECT DISTINCT user_id, device_id
                        FROM user_daily_visits
                        WHERE user_id IN %s
                        AND timestamp >= %s
                        AND device_id IS NOT NULL
                    ) SELECT ud.user_id, ud.device_id, ui.user_agent
                        FROM user_devices AS ud
                        LEFT JOIN user_ips AS ui
                        ON ud.user_id = ui.user_id AND ud.device_id = ui.device_id
                    UNION
                      SELECT ud.user_id, ud.device_id, d.user_agent
                        FROM user_devices AS ud
                        LEFT JOIN devices AS d
                        ON ud.user_id = d.user_id AND ud.device_id = d.device_id
                    """

    begin = time.time()
    with CONFIG.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                cohort_sql,
                (tuple(users), int(start)),
            )
            res = cursor.fetchall()
    conn.close()

    # Running this query on secondary database not tuned for long running queries
    # this is really heavy handed way of allowing background processes to keep up :/
    pause = time.time() - begin
    time.sleep(pause)
    return res


def construct_users_and_devices_to_clients_mapping(
        users_devices_user_agents: Iterable[Tuple[str, str, Optional[str]]]
) -> Dict[str, str]:
    """Build a map of user_id+device_id to client type

    Args:
        users_devices_user_agents: A list of (user_id, device_id, user_agent) triplets.

    Returns:
        A map from `<user_id>+<device_id>` to client type
    """
    users_and_devices_to_clients = {}
    for (user, device_id, user_agent) in users_devices_user_agents:
        user_id = user + "+" + device_id
        client = user_agent_to_client(user_agent)

        if user_id not in users_and_devices_to_clients:
            users_and_devices_to_clients[user_id] = client
        else:
            if client == MISSING:
                continue

            previous_client = users_and_devices_to_clients[user_id]
            if previous_client == MISSING:
                users_and_devices_to_clients[user_id] = client
            elif previous_client != client:
                logging.warning(
                    f"{user}/{device_id} used both {previous_client!r} and {client!r} "
                    f"in the window. Ignoring {client!r}"
                )

    return users_and_devices_to_clients


def estimate_client_types(client_types: Mapping[str, int]) -> Mapping[str, int]:
    """Split MISSING clients according to the proportion of known clients"""
    element_android_count = client_types.get(ELEMENT_ANDROID, 0)
    riotx_android_count = client_types.get(RIOTX_ANDROID, 0)
    element_electron_count = client_types.get(ELEMENT_ELECTRON, 0)
    element_ios_count = client_types.get(ELEMENT_IOS, 0)
    other_count = client_types.get(OTHER, 0)
    web_count = client_types.get(WEB, 0)

    missing_count = client_types.get(MISSING, 0)

    if missing_count > 0:
        count_known = element_android_count + riotx_android_count + element_electron_count + element_ios_count + other_count + web_count

        if count_known > 0:
            element_android_count = element_android_count + int((element_android_count / count_known) * missing_count)
            riotx_android_count = riotx_android_count + int((riotx_android_count / count_known) * missing_count)
            element_electron_count = element_electron_count + int((element_electron_count / count_known) * missing_count)
            element_ios_count = element_ios_count + int((element_ios_count / count_known) * missing_count)
            web_count = web_count + int((web_count / count_known) * missing_count)

    return Counter({ELEMENT_ANDROID: element_android_count,
                    RIOTX_ANDROID: riotx_android_count,
                    ELEMENT_ELECTRON: element_electron_count,
                    ELEMENT_IOS: element_ios_count,
                    WEB: web_count})


def get_cohort_users_and_client_mapping(
        cohort_start_date: int, cohort_end_date: int
) -> Tuple[Collection[User], Dict[str, str]]:
    """
    Get the users that registered an account during the given timeframe, and
    the names of all the clients they have ever used.

    Args:
        cohort_start_date: start of the timeframe to check, inclusive, as ms since the
            epoch.

        cohort_end_date: end of the timeframe to check, exclusive, as ms since the
            epoch.

    Returns:
        A pair `(users, clients)` where `users`s is the set of all users that registered
        in the given timeframe, and `clients` is a map from "user_id+device_id" to
        client type for all clients ever used by those users.
    """

    logging.info(
        f"Generating cohort between {ts_to_str(cohort_start_date)} "
        f"({cohort_start_date}) and {ts_to_str(cohort_end_date)} "
        f"({cohort_end_date})"
    )

    cohort_users = get_new_users(cohort_start_date, cohort_end_date)
    logging.info(f"Found {len(cohort_users)} users in the cohort")

    users_devices_user_agents = get_user_agents(
        tuple(u.user_id for u in cohort_users),
        cohort_start_date,
    )
    logging.info(f"users_devices_user_agents count is {len(users_devices_user_agents)}")

    users_and_devices_to_client = construct_users_and_devices_to_clients_mapping(users_devices_user_agents)
    logging.info(f"users_and_devices_to_client count is {len(users_and_devices_to_client)}")

    return (cohort_users, users_and_devices_to_client)


@attr.s(frozen=True, slots=True)
class CohortKey:
    # start date for the cohort
    cohort_start_date = attr.ib(type=str)

    # one of ELEMENT_ANDROID, WEB, etc
    client_type = attr.ib(type=str)

    # the SSO identity provider
    sso_idp = attr.ib(type=str)


@attr.s(frozen=True, slots=True)
class ClientAgnosticCohortKey:
    """
    This is like CohortKey, but doesn't mention the client type.
    The point of this is to allow tracking combined metrics (combined across
    all clients); a single user can use multiple clients and could otherwise
    appear in multiple cohorts, so a simple sum is not correct.
    """

    # start date for the cohort
    cohort_start_date = attr.ib(type=str)

    # the SSO identity provider
    sso_idp = attr.ib(type=str)


def get_cohort_clients_bucket(
    cohort_users: Collection[User],
    cohort_start_date: int,
    users_and_devices_to_client: Mapping[str, str],
    bucket_start_date: int,
    bucket_end_date: int,
) -> Iterable[Tuple[CohortKey, int]]:
    """Get the count of users who used each client type between the 2 provided dates

    Args:
        cohort_users: The cohort of users we are interested in

        cohort_start_date: the start date of the user cohort

        users_and_devices_to_client: a mapping of user_id+device_id -> client type for
            all user_id, device_id pairs we've ever seen for this cohort.
            client type will be MISSING for some user_id, device_id pairs (as user_ips
            has been reaped)

        bucket_start_date: start of the timeframe to check, inclusive, as ms since the
            epoch.

        bucket_end_date: end of the timeframe to check, exclusive, as ms since the
            epoch.

    Returns:
        A series of (cohort key, count) rows
    """
    logging.info(f"Getting client counts for cohort of size {len(cohort_users)} active between "
                 f"{ts_to_str(bucket_start_date)} and {ts_to_str(bucket_end_date)}")

    # Get a map of the device ids that were active for each user during the usage bucket
    bucket_user_device_map = get_bucket_devices_by_user(
        tuple(u.user_id for u in cohort_users), bucket_start_date, bucket_end_date
    )

    # build a list of users for each client, to deduplicate users
    # map from client to a set of (user_id, sso_idp) pairs
    clients_to_users = {}  # type: Dict[str, Set[Tuple[str, str]]]
    for user in cohort_users:
        # the user might have registered with more than one SSO IdP; if so, we just
        # pick the first one.
        sso_idp = user.auth_providers[0] if user.auth_providers else ''

        for device_id in bucket_user_device_map.get(user.user_id, []):
            client = users_and_devices_to_client[user.user_id + "+" + device_id]
            clients_to_users.setdefault(client, set()).add((user.user_id, sso_idp))

    # Now, for each SSO IdP, build a count of users per client.
    #
    # Note that if a given user uses two different clients, that one user will be
    # counted under both clients. That means that totalling retention stats across
    # clients isn't statistically correct.

    # sso_idp -> client -> count
    sso_bucket_client_types = defaultdict(Counter)   # type: Mapping[str, Counter]

    for client, users in clients_to_users.items():
        for user_id, sso_idp in users:
            sso_bucket_client_types[sso_idp][client] += 1
    logging.info(f"bucket_client_types={sso_bucket_client_types}")

    for sso_idp, client_types in sso_bucket_client_types.items():
        for client, count in estimate_client_types(client_types).items():
            cohort_key = CohortKey(ts_to_str(cohort_start_date), client, sso_idp)
            yield cohort_key, count


def get_cohort_combined_bucket(
    cohort_users: Collection[User],
    cohort_start_date: int,
    bucket_start_date: int,
    bucket_end_date: int,
) -> Iterable[Tuple[ClientAgnosticCohortKey, int]]:
    """Get the count of users who used ANY client between the 2 provided dates

    Args:
        cohort_users: The cohort of users we are interested in

        cohort_start_date: the start date of the user cohort

        bucket_start_date: start of the timeframe to check, inclusive, as ms since the
            epoch.

        bucket_end_date: end of the timeframe to check, exclusive, as ms since the
            epoch.

    Returns:
        A series of (client-agnostic cohort key, count) rows
    """
    logging.info(f"Getting client counts for cohort of size {len(cohort_users)} active between "
                 f"{ts_to_str(bucket_start_date)} and {ts_to_str(bucket_end_date)}")

    # Get a map of the device ids that were active for each user during the usage bucket
    bucket_user_device_map = get_bucket_devices_by_user(
        tuple(u.user_id for u in cohort_users), bucket_start_date, bucket_end_date
    )

    # Build a list of users, to deduplicate users
    # a set of (user_id, sso_idp) pairs
    users: Set[Tuple[str, str]] = set()
    for user in cohort_users:
        if user not in bucket_user_device_map:
            # this user was not active in this part
            # XXX i think we want this. continue
            pass

        # the user might have registered with more than one SSO IdP; if so, we just
        # pick the first one.
        sso_idp: str = user.auth_providers[0] if user.auth_providers else ''

        users.add((user.user_id, sso_idp))

    # Now, for each SSO IdP, build a count of users (regardless of client).
    # sso_idp -> count
    sso_bucket = Counter()
    for (user_id, sso_idp) in users:
        sso_bucket[sso_idp] += 1
    # XXX logging.info(f"bucket_client_types={sso_bucket_client_types}")

    for sso_idp, count in sso_bucket.items():
        cohort_key = ClientAgnosticCohortKey(ts_to_str(cohort_start_date), sso_idp)
        yield cohort_key, count


# the result type of the generate methods.
# A set of (cohort key, bucket number, count) rows
CohortStatsResult = Iterable[Tuple[CohortKey, int, int]]

# XXX
ClientAgnosticCohortStatsResult = Iterable[Tuple[ClientAgnosticCohortKey, int, int]]


def generate_by_cohort(
    cohort_start_date: int, buckets: int, period: int
) -> CohortStatsResult:
    """
    Generate the stats for the given cohort across multiple buckets

    Args:
        cohort_start_date: start time of cohort bucket to update
        buckets: number of usage buckets to update
        period: duration of each cohort/bucket (ms)

    Returns:
        a CohortStatsResult
    """
    cohort_end_date = cohort_start_date + period

    now = int(time.time()) * 1000
    if (now - cohort_start_date) < buckets * period:
        buckets = int((now - cohort_start_date) / period)

    logging.info(
        f"Tracking cohort of {ts_to_str(cohort_start_date)} to "
        f"{ts_to_str(cohort_end_date)}, for {buckets} activity buckets each of "
        f"{period / MS_PER_DAY} days."
    )

    cohort_users, users_and_devices_to_client = get_cohort_users_and_client_mapping(cohort_start_date,
                                                                                    cohort_end_date)

    for bucket in range(buckets):
        bucket_num = bucket + 1
        bucket_start_date = cohort_start_date + (bucket * period)
        bucket_end_date = bucket_start_date + period

        client_types = get_cohort_clients_bucket(
            cohort_users,
            cohort_start_date,
            users_and_devices_to_client,
            bucket_start_date,
            bucket_end_date,
        )

        for cohort_key, count in client_types:
            yield cohort_key, bucket_num, count


def generate_by_bucket(
    bucket_start_date: int, buckets: int, period: int
) -> CohortStatsResult:
    """
    Generate the stats for each user cohort from the usage stats in the given bucket

    Args:
        bucket_start_date: start time of usage bucket to inspect (ms since the epoch,
           inclusive)
        buckets: number of user cohorts to update
        period: duration of each cohort/bucket (ms)

    Returns:
        a CohortStatsResult
    """
    bucket_end_date = bucket_start_date + period
    logging.info(
        f"Updating usage stats for each of {buckets} cohorts of "
        f"{period / MS_PER_DAY} days, for activity between "
        f"{ts_to_str(bucket_start_date)} and {ts_to_str(bucket_end_date)}"
    )

    # If we request n buckets, then the cohort n-1 back will have its n'th bucket at bucket_start_date
    for bucket in range(buckets):
        cohort_start_date = bucket_start_date - (bucket * period)
        cohort_end_date = cohort_start_date + period
        cohort_users, users_and_devices_to_client = get_cohort_users_and_client_mapping(cohort_start_date,
                                                                                        cohort_end_date)

        bucket_num = bucket + 1
        client_types = get_cohort_clients_bucket(
            cohort_users,
            cohort_start_date,
            users_and_devices_to_client,
            bucket_start_date,
            bucket_end_date,
        )
        for cohort_key, count in client_types:
            yield cohort_key, bucket_num, count


def write_to_mysql(table: str, buckets_stats: CohortStatsResult, dry_run: bool):
    statements_and_values = []

    for cohort_key, bucket_num, count in buckets_stats:
        insert_bucket = f"""\
            INSERT INTO {table} (date, client, sso_idp, b{bucket_num})
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE b{bucket_num}=VALUES(b{bucket_num})
        """
        statements_and_values.append((
            insert_bucket, (
                cohort_key.cohort_start_date,
                cohort_key.client_type,
                cohort_key.sso_idp,
                count
            )
        ))

    if dry_run:
        logging.info("Would have run the following SQL statements")
        for insert_string, values in statements_and_values:
            print(insert_string % values)
        return

    # Connect to and setup db:
    db = MySQLdb.connect(
        host=CONFIG.STATS_DB_HOST,
        user=CONFIG.STATS_DB_USERNAME,
        passwd=CONFIG.STATS_DB_PASSWORD,
        db=CONFIG.STATS_DB_DATABASE,
        port=3306,
        ssl='ssl'
    )

    with db.cursor() as cursor:
        for insert_string, values in statements_and_values:
            cursor.execute(insert_string, values)
        db.commit()


CONFIG = Config()


def main():
    args = get_args()
    mode, date, buckets, period, table, dry_run = parse_cohort_parameters(args)

    if mode == "cohort":
        buckets_stats = generate_by_cohort(date, buckets, period)
    elif mode == "bucket":
        buckets_stats = generate_by_bucket(date, buckets, period)
    else:
        raise ValueError(f"Unexpected mode {mode}")

    write_to_mysql(table, buckets_stats, dry_run)


if __name__ == '__main__':
    main()
