import time
from psycopg2 import connect


import datetime
import argparse
import logging
import yaml
import MySQLdb
from os.path import expanduser

# Script to calculate user retention cohorts and output the results,
# comma separated to stdout

ELECTRON = "electron"
WEB = "web"
ANDROID = "android"
IOS = "ios"
ONE_DAY = 86400000
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.ERROR)


class Config:
    def __init__(self):
        with open(expanduser("~") + "/.stats", "r") as config_file:
            config = yaml.safe_load(config_file)
            self.DB_NAME = config["db_name"]
            self.DB_USER = config["db_user"]
            self.DB_PASSWORD = config["db_password"]
            self.DB_HOST = config["db_host"]
            self.MYSQL_PASSWORD = config["mysql_password"]

    def get_conn(self):

        conn = connect(
            dbname=CONFIG.DB_NAME,
            user=CONFIG.DB_USER,
            password=CONFIG.DB_PASSWORD,
            host=CONFIG.DB_HOST,
            options="-c search_path=matrix",
        )
        conn.set_session(readonly=True, autocommit=True)
        return conn


class Helper:
    """Misc helper methods"""

    @staticmethod
    def create_table(db, schema):
        """This method executes a CREATE TABLE IF NOT EXISTS command
        _without_ generating a mysql warning if the table already exists."""
        cursor = db.cursor()
        cursor.execute('SET sql_notes = 0;')
        cursor.execute(schema)
        cursor.execute('SET sql_notes = 1;')
        db.commit()


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
        "-s",
        "--startdate",
        type=lambda d: datetime.datetime.strptime(d, "%Y-%m-%d"),
        required=True,
        help="Beginning of first cohort in form %Y-%m-%d",
    )
    ap.add_argument(
        "-p",
        "--period",
        help="Period over which cohorts are calculated, measured in days. Defaults to 7",
    )
    ap.add_argument(
        "-b", "--buckets", help="How many time buckets for each cohort, defaults to 6"
    )

    return ap.parse_args()


def parse_cohort_parameters(args):

    start_date = int(args.startdate.strftime("%s")) * 1000
    period = 7 * 24 * 60 * 60 * 1000

    if args.period:
        period = int(args.period) * 24 * 60 * 60 * 1000

    now = int(time.time()) * 1000
    buckets = 6
    if args.buckets is not None:
        buckets = int(args.buckets)
    if (now - start_date) < buckets * period:
        buckets = int((now - start_date) / period)

    # end_date = start_date + period * buckets
    end_date = int(time.time() * 1000)

    return start_date, end_date, buckets, period


def user_agent_to_client(user_agent):

    ua = user_agent.lower()

    if "electron" in ua:
        return ELECTRON
    elif "mozilla" in ua or "gecko" in ua:
        return WEB
    elif "android" in ua:
        return ANDROID
    elif "ios" in ua:
        return IOS
    elif "synapse" in ua or "okhttp" in ua or "python-requests" in ua:
        pass
    else:
        # print("Could not identify UA %s" % ua)
        pass
    return None


# select all users that created an account for a given range
def get_new_users(start_date, period):
    new_user_sql = """
    SELECT DISTINCT users.name, udv.device_id, uip.user_agent FROM users
    LEFT JOIN user_daily_visits as udv
    ON users.name=udv.user_id
    LEFT JOIN user_ips as uip
    ON users.name = uip.user_id
    where appservice_id is NULL
    AND is_guest=0
    AND creation_ts >= %(start_date_seconds)s
    AND udv.timestamp >= %(start_date)s
    AND creation_ts < %(end_date_seconds)s
    AND udv.timestamp < %(end_date)s
    AND udv.device_id=uip.device_id
    """
    start = time.time()
    with CONFIG.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                new_user_sql,
                {
                    "start_date_seconds": start_date / 1000,
                    "start_date": start_date,
                    "end_date_seconds": (start_date + period) / 1000,
                    "end_date": start_date + period,
                },
            )
            # print('row count is %d' % cursor.rowcount)
            res = cursor.fetchall()
    conn.close()
    # Running this query on secondary database not tuned for long running queries
    # this is really heavy handed way of allowing background processes to keep up :/
    pause = time.time() - start
    time.sleep(pause)
    return res


def get_cohort_buckets(users, start, stop, client):

    # Including client is unnecessary other than as a hack to aid testing :/
    cohort_sql = """ SELECT DISTINCT udv.user_id, udv.device_id, uip.user_agent
                        FROM user_daily_visits as udv
                        LEFT JOIN user_ips as uip
                        ON udv.user_id = uip.user_id
                        WHERE udv.user_id IN %s
                        AND udv.timestamp >= %s and udv.timestamp < %s
                        AND udv.device_id = uip.device_id
                    """

    now = time.time()
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
    pause = time.time() - now
    time.sleep(pause)
    return res


def filter_users_by_client(all_users_devices):
    """converts user_id device_ids into client types

    Args:
        all_users_devices ([tuple(str,str)]): a list of (user_id,device_id) tuples

    Returns:
        dict[str:set]: maps client type to user_ids
    """
    if len(all_users_devices) == 0:
        return {}
    filtered_users = {ELECTRON: set(), WEB: set(), IOS: set(), ANDROID: set()}

    for u in all_users_devices:
        user_id = u[0]
        user_agent = u[2]
        client_type = user_agent_to_client(user_agent)
        if client_type:
            filtered_users[client_type].add(user_id)
    return filtered_users


def generate_cohorts(start_date, end_date, buckets, period):
    cohort_date = start_date
    result = {}
    logging.info(
        'cohort date %s end_date %s period %d'
        % (ts_to_str(cohort_date), ts_to_str(end_date), period)
    )
    while cohort_date <= end_date - period:
        logging.info(
            'cohort date %s end_date %s period %d'
            % (ts_to_str(cohort_date), ts_to_str(end_date), period)
        )

        try:
            all_users_devices = get_new_users(cohort_date, period)
        except:
            logging.error('XXXXXXXX error calling all_user_devices XXXXXXX')
            time.sleep(300)
            all_users_devices = get_new_users(cohort_date, period)
        logging.info('all_users_devices count is %d' % len(all_users_devices))
        filtered_users = filter_users_by_client(all_users_devices)
        for client_type, users in filtered_users.items():
            cohort_scores = [len(users)]
            cohort_bucket_start = cohort_date + period

            while (
                cohort_bucket_start < cohort_date + (buckets * period)
                and cohort_bucket_start <= end_date - period
            ):
                logging.info("calling get_cohort_buckets %s" % client_type)
                try:
                    user_device_ids = get_cohort_buckets(
                        users,
                        cohort_bucket_start,
                        cohort_bucket_start + period,
                        client_type)
                except:
                    logging.error('error calling get_cohort_buckets')
                    time.sleep(300)
                    user_device_ids = get_cohort_buckets(
                        users,
                        cohort_bucket_start,
                        cohort_bucket_start + period,
                        client_type)

                count = 0
                users_seen = set()

                # hack to get drop off mxids
                # if client_type == 'web':
                #     dropped_off_user = set(users) - set([x[0] for x in user_device_ids])
                #     logging.info('Cohort date %s bucket %s size %d \n user list %s' % (
                #         ts_to_str(cohort_date), ts_to_str(cohort_bucket_start), len(dropped_off_user), dropped_off_user)
                #     )
                logging.info('user_device_ids is %d' % len(user_device_ids))
                for u in user_device_ids:
                    if len(u) != 3:
                        continue
                    client = user_agent_to_client(u[2])
                    if client is client_type and u[0] not in users_seen:
                        users_seen.add(u[0])
                        count = count + 1
                cohort_scores.append(count)
                cohort_bucket_start = cohort_bucket_start + period
            cohort_data = result.get(client_type, [])
            cohort_data.append((ts_to_str(cohort_date), cohort_scores))
            result[client_type] = cohort_data

        cohort_date = cohort_date + period
    return result


def write_to_mysql(period, all_cohorts):
    # TODO select db based on period

    # TODO handle updating data where data pre-exists
    # TABLE_NAME = 'cohorts_weekly'
    if period == 1:
        table = 'cohorts_daily'
    elif period == 7:
        table = 'cohorts_weekly'
    elif period == 30:
        table = 'cohorts_monthly'
    else:
        logging.error('Unsupported period, must be 1, 7 or 30: %s' % period)
    # SCHEMA = """
    # CREATE TABLE IF NOT EXISTS
    # %s (
    #     date DATE NOT NULL,
    #     client VARCHAR(12) NOT NULL,
    #     b1 INT NOT NULL DEFAULT '0', b2 INT NOT NULL DEFAULT '0',
    #     b3 INT NOT NULL DEFAULT '0', b4 INT NOT NULL DEFAULT '0',
    #     b5 INT NOT NULL DEFAULT '0', b6 INT NOT NULL DEFAULT '0',
    #     b7 INT NOT NULL DEFAULT '0', b8 INT NOT NULL DEFAULT '0',
    #     b9 INT NOT NULL DEFAULT '0', b10 INT NOT NULL DEFAULT '0',
    #     b11 INT NOT NULL DEFAULT '0', b12 INT NOT NULL DEFAULT '0'
    # );
    # """ % TABLE_NAME

    # Connect to and setup db:
    db = MySQLdb.connect(
        host='localhost',
        user='businessmetrics',
        passwd=CONFIG.MYSQL_PASSWORD,
        db='businessmetrics',
        port=3306
    )

    with db.cursor() as cursor:

        delete_entries = "DELETE FROM " + table

        cursor.execute(delete_entries)
        db.commit()
        insert_entries = "INSERT INTO " + table + \
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

        for client, cohorts in all_cohorts.items():
            for cohort in cohorts:
                date = cohort[0]
                r = cohort[1]

                while len(r) < 12:
                    r.append(int(0))
                # TODO This mings, collapse to single array
                cursor.execute(
                    insert_entries,
                    (
                        date,
                        client,
                        r[0], r[1], r[2], r[3], r[4], r[5],
                        r[6], r[7], r[8], r[9], r[10], r[11]
                    )
                )
        db.commit()


CONFIG = Config()


def main():
    args = get_args()
    start_date, end_date, buckets, period = parse_cohort_parameters(args)
    period_human = period / (24 * 60 * 60 * 1000)
    logging.info(
        "Start Date: %s bucket size %d period %s End Date %s"
        % (start_date, buckets, period_human, end_date)
    )
    res = generate_cohorts(start_date, end_date, buckets, period)
    logging.info(res)
    write_to_mysql(period_human, res)


if __name__ == '__main__':
    main()
