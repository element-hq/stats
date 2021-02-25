#!/usr/bin/env python3

import argparse
import datetime
import logging
import os
import time
from collections import Counter
from typing import (Collection, Dict, Iterable, List, Mapping, Optional,
                    Sequence, Tuple)

import MySQLdb
from psycopg2 import connect

# Script to calculate user retention cohorts and output the results,
# comma separated to stdout

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
        self.SYNAPSE_DB_HOST = os.environ["SYNAPSE_DB_HOST"]
        self.SYNAPSE_DB_USERNAME = os.environ["SYNAPSE_DB_USERNAME"]
        self.SYNAPSE_DB_PASSWORD = os.environ["SYNAPSE_DB_PASSWORD"]
        self.SYNAPSE_DB_DATABASE = os.environ["SYNAPSE_DB_DATABASE"]
        self.SYNAPSE_DB_OPTIONS = os.environ["SYNAPSE_DB_OPTIONS"]
        self.STATS_DB_HOST = os.environ["STATS_DB_HOST"]
        self.STATS_DB_USERNAME = os.environ["STATS_DB_USERNAME"]
        self.STATS_DB_PASSWORD = os.environ["STATS_DB_PASSWORD"]
        self.STATS_DB_DATABASE = os.environ["STATS_DB_DATABASE"]

    def get_conn(self):
        conn = connect(
            dbname=self.SYNAPSE_DB_DATABASE,
            user=self.SYNAPSE_DB_USERNAME,
            password=self.SYNAPSE_DB_PASSWORD,
            host=self.SYNAPSE_DB_HOST,
            options=self.SYNAPSE_DB_OPTIONS,
        )
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


def get_new_users(start: int, stop: int) -> Sequence[Tuple[str, str]]:
    """Get a list of all users that registered an account during
    the given timeframe

    Also gets the IDs of any devices that they used during that period.

    Args:
        start: start of the timeframe to check, inclusive, as ms since the
            epoch.

        stop: end of the timeframe to check, exclusive, as ms since the
            epoch.
    Returns:
        A sequence of (user id, device id) pairs.
    """

    # XXX we should drop device_id from the result, given it is unused so we'll just
    #    end up returning redundant rows for the same user.
    #
    # XXX not quite sure why we join against user_daily_visits at all. Possibly to
    #    filter out users who managed to register, but have never used the account, so
    #    don't have a device in user_daily_visits? Likewise we appear to exclude users
    #    who registered on a given day but didn't actually use that account on the first
    #    day - it's unclear if this is intentional, and if so why.

    new_user_sql = """ SELECT DISTINCT users.name, udv.device_id
                        FROM users
                        LEFT JOIN user_daily_visits as udv
                        ON users.name = udv.user_id
                        WHERE appservice_id is NULL
                        AND is_guest = 0
                        AND creation_ts >= %(start_date_seconds)s
                        AND udv.timestamp >= %(start_date)s
                        AND creation_ts < %(end_date_seconds)s
                        AND udv.timestamp < %(end_date)s
                        AND udv.device_id IS NOT NULL
                    """

    begin = time.time()
    with CONFIG.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                new_user_sql,
                {
                    "start_date_seconds": start / 1000,
                    "start_date": start,
                    "end_date_seconds": stop / 1000,
                    "end_date": stop,
                },
            )
            # print('row count is %d' % cursor.rowcount)
            res = cursor.fetchall()
    conn.close()

    # Running this query on secondary database not tuned for long running queries
    # this is really heavy handed way of allowing background processes to keep up :/
    pause = time.time() - begin
    time.sleep(pause)
    return res


def get_cohort_user_devices_bucket(
    users: Collection[str], start: int, stop: int
) -> Sequence[Tuple[str, str]]:
    """Given a list of users, get the device IDs that they used in the given period
    """
    if len(users) == 0:
        return []
    cohort_sql = """SELECT DISTINCT user_id, device_id
                        FROM user_daily_visits
                        WHERE user_id IN %s
                        AND timestamp >= %s and timestamp < %s
                        AND device_id IS NOT NULL
                    """

    begin = time.time()
    with CONFIG.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                cohort_sql,
                (tuple(users), int(start), int(stop)),
            )
            res = cursor.fetchall()
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


def map_users_devices_to_clients(
    users_devices: Iterable[Tuple[str, str]], users_and_devices_to_clients: Mapping[str, str]
) -> Mapping[str, int]:
    """given a list of users and devices, calculate the number of users on each client

    Note that if a given user uses two different clients, that one user will be counted
    under both clients. That means that totalling retention stats across clients isn't
    statistically correct.
    """

    # first build a list of users for each client, to deduplicate users
    clients_to_users = {}
    for (user, device_id) in users_devices:
        client = users_and_devices_to_clients[user + "+" + device_id]
        clients_to_users.setdefault(client, set()).add(user)

    # then convert to a count of users per client
    counts = Counter()
    for client, users in clients_to_users.items():
        counts[client] = len(users)
    return counts


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
) -> Tuple[Collection[str], Dict[str, str]]:
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

    cohort_users_devices = get_new_users(cohort_start_date, cohort_end_date)
    logging.info(f"cohort_users_devices count is {len(cohort_users_devices)}")

    cohort_users = set([user_device[0] for user_device in cohort_users_devices])
    logging.info(f"cohort_users count is {len(cohort_users)}")

    users_devices_user_agents = get_user_agents(cohort_users, cohort_start_date)
    logging.info(f"users_devices_user_agents count is {len(users_devices_user_agents)}")

    users_and_devices_to_client = construct_users_and_devices_to_clients_mapping(users_devices_user_agents)
    logging.info(f"users_and_devices_to_client count is {len(users_and_devices_to_client)}")

    return (cohort_users, users_and_devices_to_client)


def get_cohort_clients_bucket(
        cohort_users: Collection[str],
        users_and_devices_to_client: Mapping[str, str],
        bucket_start_date: int,
        bucket_end_date: int,
) -> Mapping[str, int]:
    """Get the count of users who used each client type between the 2 provided dates

    Args:
        cohort_users: The cohort of users we are interested in

        users_and_devices_to_client: a mapping of user_id+device_id -> client type for
            all user_id, device_id pairs we've ever seen for this cohort.
            client type will be MISSING for some user_id, device_id pairs (as user_ips
            has been reaped)

        bucket_start_date: start of the timeframe to check, inclusive, as ms since the
            epoch.

        bucket_end_date: end of the timeframe to check, exclusive, as ms since the
            epoch.

    Returns:
        Mapping from client type to number of users
    """
    logging.info(f"Getting client counts for cohort of size {len(cohort_users)} active between "
                 f"{ts_to_str(bucket_start_date)} and {ts_to_str(bucket_end_date)}")

    # All user_devices of the above that are still active in cohort_date
    bucket_users_devices = get_cohort_user_devices_bucket(cohort_users, bucket_start_date, bucket_end_date)
    logging.info(f"bucket_users_devices count is {len(bucket_users_devices)}")

    bucket_client_types = map_users_devices_to_clients(bucket_users_devices, users_and_devices_to_client)
    logging.info(f"bucket_client_types={bucket_client_types}")

    estimated_client_types = estimate_client_types(bucket_client_types)
    logging.info(f"estimated_client_types={estimated_client_types}")
    return estimated_client_types


def generate_by_cohort(cohort_start_date, buckets, period):
    cohort_end_date = cohort_start_date + period

    now = int(time.time()) * 1000
    if (now - cohort_start_date) < buckets * period:
        buckets = int((now - cohort_start_date) / period)

    logging.info(
        f"Tracking cohort of {ts_to_str(cohort_start_date)} to "
        f"{ts_to_str(cohort_end_date)}, for {buckets} activity buckets each of "
        f"{period / MS_PER_DAY} days."
    )

    cohorts = []
    cohort_users, users_and_devices_to_client = get_cohort_users_and_client_mapping(cohort_start_date,
                                                                                    cohort_end_date)

    for bucket in range(buckets):
        bucket_num = bucket + 1
        bucket_start_date = cohort_start_date + (bucket * period)
        bucket_end_date = bucket_start_date + period

        client_types = get_cohort_clients_bucket(cohort_users, users_and_devices_to_client,
                                                 bucket_start_date, bucket_end_date)
        cohorts.append((ts_to_str(cohort_start_date), bucket_num, client_types))

    return cohorts


def generate_by_bucket(
        bucket_start_date: int, buckets: int, period: int
) -> List[Tuple[str, int, Mapping[str, int]]]:
    """
    Generate the stats for each user cohort from the usage stats in the given bucket

    Args:
        bucket_start_date: start time of usage bucket to inspect (ms since the epoch,
           inclusive)
        buckets: number of user cohorts to update
        period: duration of each cohort/bucket (ms)

    Returns:
        For each cohort:
          (start date for the cohort, bucket number for the cohort, client type->count map)
    """
    bucket_end_date = bucket_start_date + period
    logging.info(
        f"Updating usage stats for each of {buckets} cohorts of "
        f"{period / MS_PER_DAY} days, for activity between "
        f"{ts_to_str(bucket_start_date)} and {ts_to_str(bucket_end_date)}"
    )

    cohorts = []

    # If we request n buckets, then the cohort n-1 back will have its n'th bucket at bucket_start_date
    for bucket in range(buckets):
        cohort_start_date = bucket_start_date - (bucket * period)
        cohort_end_date = cohort_start_date + period
        cohort_users, users_and_devices_to_client = get_cohort_users_and_client_mapping(cohort_start_date,
                                                                                        cohort_end_date)

        bucket_num = bucket + 1
        client_types = get_cohort_clients_bucket(cohort_users, users_and_devices_to_client,
                                                 bucket_start_date, bucket_end_date)
        cohorts.append((ts_to_str(cohort_start_date), bucket_num, client_types))

    return cohorts


def write_to_mysql(table, buckets_stats, dry_run):
    statements_and_values = []
    for cohort_date, bucket_num, client_counts in buckets_stats:
        for client in [ELEMENT_ANDROID, RIOTX_ANDROID, ELEMENT_ELECTRON, ELEMENT_IOS, WEB]:
            insert_bucket = f"INSERT INTO {table} (date, client, b{bucket_num}) VALUES" \
                            f"(%s, %s, %s) " \
                            f"ON DUPLICATE KEY UPDATE b{bucket_num}=VALUES(b{bucket_num});"
            statements_and_values.append((insert_bucket, cohort_date, client, client_counts[client]))

    if dry_run:
        logging.info("Would have run the following SQL statements")
        for insert_string, cohort_date, client, count in statements_and_values:
            print(insert_string % (cohort_date, client, count))
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
        for insert_string, cohort_date, client, count in statements_and_values:
            cursor.execute(insert_string, (cohort_date, client, count))
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

    logging.info(buckets_stats)
    write_to_mysql(table, buckets_stats, dry_run)


if __name__ == '__main__':
    main()
