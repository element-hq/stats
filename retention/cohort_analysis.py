#!/usr/bin/env python3

import time
from psycopg2 import connect

from collections import Counter
import argparse
import datetime
import logging
import MySQLdb
import os

# Script to calculate user retention cohorts and output the results,
# comma separated to stdout

ELEMENT_ELECTRON = "electron"
WEB = "web"
ELEMENT_ANDROID = "android"
RIOTX_ANDROID = "android-riotx"
ELEMENT_IOS = "ios"
MISSING = "missing"
OTHER = "other"

ONE_DAY = 86400000
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
        help="Period over which cohorts / buckets are calculated, measured in days. Defaults to 7",
    )
    ap.add_argument(
        "-b",
        "--buckets",
        type=int,
        default=6,
        help="How many time buckets for each cohort, defaults to 6",
    )

    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--cohort_start_date",
        type=lambda d: datetime.datetime.strptime(d, "%Y-%m-%d"),
        help="Beginning of first cohort in the form %Y-%m-%d. "
             "Will generate all buckets in this cohort")

    group.add_argument(
        "--bucket_start_date",
        type=lambda d: datetime.datetime.strptime(d, "%Y-%m-%d"),
        help="Beginning of a bucket in the form %Y-%m-%d. "
             "Will generate all buckets that have this bucket start date")

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
    period = int(period) * 24 * 60 * 60 * 1000

    now = int(time.time()) * 1000
    if (date + period) > now:
        raise ValueError(f"{date} is too soon, 0 periods will fit between it and now")

    return mode, date, args.buckets, period, table


def user_agent_to_client(user_agent):
    if user_agent is None or len(user_agent) == 0:
        return MISSING

    ua = user_agent.lower()
    if "riot" or "electron" in ua:
        if "electron" in ua:
            return RIOT_ELECTRON
        elif "android" in ua and "riotx" in ua:
                return RIOTX_ANDROID
        elif "android" in ua:
            return ELECTRON_ANDROID
        elif "ios" in ua:
            return ELECTRON_IOS
    elif "mozilla" in ua or "gecko" in ua:
        return WEB
    elif "synapse" in ua or "okhttp" in ua or "python-requests" in ua:
        # Never consider this for over-writing of any other client type
        return MISSING

    return OTHER


# select all users that created an account for a given range
def get_new_users(start, stop):
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


def get_cohort_user_devices_bucket(users, start, stop):
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


def get_user_agents(users, start):
    if len(users) == 0:
        return []
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


def construct_users_and_devices_to_clients_mapping(users_devices_user_agents):
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
                logging.warning(f"{user}/{device_id} changed from "
                                f"{previous_client} to {client}. Ignoring")

    return users_and_devices_to_clients


def map_users_devices_to_clients(users_devices, users_and_devices_to_clients):
    clients_to_users = {}
    for (user, device_id) in users_devices:
        client = users_and_devices_to_clients[user + "+" + device_id]
        clients_to_users.setdefault(client, set()).add(user)

    counts = Counter()
    for client, users in clients_to_users.items():
        counts[client] = len(users)
    return counts


def estimate_client_types(client_types):
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


# Grabs the users in a cohort (joined between the 2 dates)
# Also returns a mapping of user_id+device_id -> client type for any device a user in this cohort has ever used
def get_cohort_users_and_client_mapping(cohort_start_date, cohort_end_date):
    logging.info(f"Generating cohort between {ts_to_str(cohort_start_date)} "
                 f"and {ts_to_str(cohort_end_date)}")

    cohort_users_devices = get_new_users(cohort_start_date, cohort_end_date)
    logging.info(f"cohort_users_devices count is {len(cohort_users_devices)}")

    cohort_users = set([user_device[0] for user_device in cohort_users_devices])
    logging.info(f"cohort_users count is {len(cohort_users)}")

    users_devices_user_agents = get_user_agents(cohort_users, cohort_start_date)
    logging.info(f"users_devices_user_agents count is {len(users_devices_user_agents)}")

    users_and_devices_to_client = construct_users_and_devices_to_clients_mapping(users_devices_user_agents)
    logging.info(f"users_and_devices_to_client count is {len(users_and_devices_to_client)}")

    return (cohort_users, users_and_devices_to_client)


# Given a cohort of users return the count of users who used one of the client types we care about between the 2 provided dates
# We provide a mapping of user_id+device_id -> client type for all user_id, device_id pairs we've ever seen for this cohort
# The client type will be MISSING for some user_id, device_id pairs (as user_ips has been reaped)
# Estimate the proportion of each client type by assuming they're in the same ratio as the present client types
def get_cohort_clients_bucket(cohort_users, users_and_devices_to_client, bucket_start_date, bucket_end_date):
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

    period_human = period / (24 * 60 * 60 * 1000)
    logging.info(f"Start Date: {ts_to_str(cohort_start_date)} to {ts_to_str(cohort_end_date)}. "
                 f"Bucket size {buckets} of {period_human} days")

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


def generate_by_bucket(bucket_start_date, buckets, period):
    bucket_end_date = bucket_start_date + period
    logging.info(f"Generating cohorts for users active between {ts_to_str(bucket_start_date)}"
                 f" and {ts_to_str(bucket_end_date)} for {buckets} cohorts")

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


def write_to_mysql(table, buckets_stats):
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
        for cohort_date, bucket_num, client_counts in buckets_stats:
            for client in [ELEMENT_ANDROID, RIOTX_ANDROID, ELEMENT_ELECTRON, ELEMENT_IOS, WEB]:
                insert_bucket = f"INSERT INTO {table} (date, client, b{bucket_num}) VALUES" \
                                f"(%s, %s, %s) " \
                                f"ON DUPLICATE KEY UPDATE b{bucket_num}=VALUES(b{bucket_num});"
                # print(insert_bucket % (cohort_date, client, client_counts[client]))
                cursor.execute(insert_bucket, (cohort_date, client, client_counts[client]))
        db.commit()


CONFIG = Config()


def main():
    args = get_args()
    mode, date, buckets, period, table = parse_cohort_parameters(args)

    if mode == "cohort":
        buckets_stats = generate_by_cohort(date, buckets, period)
    elif mode == "bucket":
        buckets_stats = generate_by_bucket(date, buckets, period)
    else:
        raise ValueError(f"Unexpected mode {mode}")

    logging.info(buckets_stats)
    write_to_mysql(table, buckets_stats)


if __name__ == '__main__':
    main()
