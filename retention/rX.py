from psycopg2 import connect
import yaml

from os.path import expanduser

import datetime
import time
from dateutil import tz
import argparse
import MySQLdb

DAY_IN_MILLIS = 24 * 60 * 60 * 1000


class Config:
    def __init__(self):
        with open(expanduser("~") + "/.stats", "r") as config_file:
            config = yaml.safe_load(config_file)
            self.DB_NAME = config["db_name"]
            self.DB_USER = config["db_user"]
            self.DB_PASSWORD = config["db_password"]
            self.DB_HOST = config["db_host"]
            self.MYSQL_PASSWORD = config["mysql_password"]


def get_args():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "-d",
        "--days",
        default=90,
        help="How many days of history are requested, defaults to 90",
    )
    ap.add_argument(
        "-r",
        "--retained_period",
        default=30,
        help="Period over which retained stats are calculated, defaults to 30",
    )
    return ap.parse_args()


def get_rx(conn, first_day, R):

    now = datetime.datetime.utcnow()
    today_start = datetime.datetime(
        now.year, now.month,
        now.day, tzinfo=tz.tzutc()
    )
    today_start_unix = int(time.mktime(today_start.timetuple())) * 1000

    sql = """
    SELECT count(*)
    FROM (
        SELECT users.name from users
        JOIN user_daily_visits ON users.name=user_daily_visits.user_id
        WHERE users.name IN (
            select user_id
            from user_daily_visits
            where timestamp <= %s
            AND timestamp > 1000 * ((%s /1000)  - (%s * 86400))
        )
        AND user_daily_visits.timestamp/1000 - users.creation_ts > 86400 * %s
        AND appservice_id IS NULL AND is_guest=0 GROUP BY users.name
    ) as u
    """
    # old = """
    # SELECT COALESCE(count(*), 0) FROM (
    #                 SELECT users.name, users.creation_ts * 1000,
    #                                                     MAX(uip.last_seen)
    #                 FROM users
    #                 INNER JOIN (
    #                     SELECT
    #                     user_id,
    #                     last_seen
    #                     FROM user_ips
    #                 ) uip
    #                 ON users.name = uip.user_id
    #                 AND appservice_id is NULL
    #                 AND users.creation_ts < ?
    #                 AND uip.last_seen/1000 > ?
    #                 AND (uip.last_seen/1000) - users.creation_ts > 86400 * 30
    #                 GROUP BY users.name, users.creation_ts
    # """

    results = {}
    day = first_day

    while day < today_start_unix:
        with conn:
            with conn.cursor() as cur:
                # This query run on a db optimised for acting as secondary and not for
                # supporting long running queries
                start = time.time()
                cur.execute(sql, (day, day, R, R))
                results[day] = cur.fetchone()
                day = day + DAY_IN_MILLIS
                time.sleep(time.time() - start)

    return results


def get_start_date(db, R):
    # 22nd May 2018
    DAWN_OF_TIME = 1526947200000
    get_last_entry = "SELECT date FROM rx WHERE period=%s ORDER BY DATE DESC LIMIT 1"
    with db.cursor() as cursor:
        cursor.execute(get_last_entry, (R,))
        date = cursor.fetchone()

        if date:
            return date[0]
        else:
            return DAWN_OF_TIME + R * DAY_IN_MILLIS


def get_mysql_db(CONFIG):
    return MySQLdb.connect(
        host='localhost',
        user='businessmetrics',
        passwd=CONFIG.MYSQL_PASSWORD,
        db='businessmetrics',
        port=3306
    )


def write_to_mysql(db, R, results):

    # SCHEMA = """
    # CREATE TABLE IF NOT EXISTS
    # %s (
    #     date INT NOT NULL,
    #     period INT NOT NULL,
    #     value INT NOT NULL DEFAULT '0'
    # );
    # """ % TABLE_NAME

    with db.cursor() as cursor:
        insert_entries = """
        INSERT INTO rx
        (date, period, value) VALUES (%s, %s, %s)
        """

        for ts, score in results.items():
            # date = datetime.datetime.fromtimestamp(ts/1000).strftime('%Y-%m-%d')

            cursor.execute(
                insert_entries,
                (
                    ts,
                    R,
                    score[0],
                )
            )
            db.commit()


def main():
    CONFIG = Config()
    try:
        conn = connect(
            dbname=CONFIG.DB_NAME,
            user=CONFIG.DB_USER,
            password=CONFIG.DB_PASSWORD,
            host=CONFIG.DB_HOST,
            options="-c search_path=matrix",
        )
    except Exception:
        print("Can't connect to psql :(")

    args = get_args()
    R = int(args.retained_period)

    mysql_db = get_mysql_db(CONFIG)
    start_day = get_start_date(mysql_db, R)
    results = get_rx(conn, start_day, R)

    write_to_mysql(mysql_db, R, results)


if __name__ == '__main__':
    main()
