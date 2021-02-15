#!/usr/bin/env python3

import csv
import datetime
import os
import sys

import psycopg2

CLIENTS = ["android", "android-riotx", "electron", "ios", "missing", "other", "web"]

# Client data collection began on 2020-10-16
# Must wait 60 days until sufficient historic data exists for client retention
CLIENT_THRESHOLD_DATE = datetime.date(2020, 12, 15)

# Goal: Count all distinct MXIDs which:
# - Appear more than once in the past 60 days
# - Have more than 30 days between the most and least recent appearances
R30_ALL_SQL = """
    SELECT
        DISTINCT COUNT(*) OVER ()
    FROM
        user_daily_visits
    WHERE
        timestamp > (extract(epoch from timestamp %(date)s - interval '60 days')::bigint * 1000)
        AND
        timestamp < (extract(epoch from timestamp %(date)s + interval '1 day')::bigint * 1000)
    GROUP BY
        user_id
    HAVING
        max(timestamp) - min(timestamp) > (extract(epoch from interval '30 days')::bigint * 1000)
    ;
"""

# Goal: Count how many MXIDs each client type has retained for more than 30 days
# Query is similar to above, but grouped by [mxid, client] instead of [mxid]
R30_CLIENT_SQL = """
    SELECT
        client_type,
        count(client_type)
    FROM
        (
            SELECT
                user_id,
                CASE
                    WHEN
                        user_agent IS NULL OR
                        user_agent = ''
                        THEN 'missing'

                    WHEN
                        user_agent ILIKE '%%riot%%' OR
                        user_agent ILIKE '%%element%%'
                        THEN CASE
                            WHEN
                                user_agent ILIKE '%%electron%%'
                                THEN 'electron'

                            WHEN
                                user_agent ILIKE '%%android%%'
                                THEN CASE
                                    WHEN
                                        user_agent ILIKE '%%riotx%%'
                                        THEN 'android-riotx'

                                    ELSE 'android'
                                END

                            WHEN
                                user_agent ILIKE '%%ios%%'
                                THEN 'ios'

                            ELSE 'other'
                        END

                    WHEN
                        user_agent ILIKE '%%mozilla%%' OR
                        user_agent ILIKE '%%gecko%%'
                        THEN 'web'

                    WHEN
                        user_agent ILIKE '%%synapse%%' OR
                        user_agent ILIKE '%%okhttp%%' OR
                        user_agent ILIKE '%%python-requests%%'
                        THEN 'missing'

                    ELSE 'other'
                END as client_type
            FROM
                user_daily_visits
            WHERE
                timestamp > (extract(epoch from timestamp %(date)s - interval '60 days')::bigint * 1000)
                AND
                timestamp < (extract(epoch from timestamp %(date)s + interval '1 day')::bigint * 1000)
            GROUP BY
                user_id,
                client_type
            HAVING
                max(timestamp) - min(timestamp) > (extract(epoch from interval '30 days')::bigint * 1000)
        ) AS temp
    GROUP BY
        client_type
    ORDER BY
        client_type
    ;
"""


def get_r30(conn, date):
    with conn:
        with conn.cursor() as curs:
            curs.execute(R30_ALL_SQL, {"date": date})
            return curs.fetchone()[0]


def get_r30_by_client(conn, date):
    with conn:
        with conn.cursor() as curs:
            curs.execute(R30_CLIENT_SQL, {"date": date})
            return dict(curs.fetchall())


def date_or_duration(s: str):
    "Convert ISO dates or '{int}d' strings to a datetime.date"
    if s.lower() == "today":
        return datetime.date.today()
    elif s.endswith("d"):
        days = int(s[:-1])
        delta = datetime.timedelta(days=days)
        return datetime.date.today() - delta
    else:
        return datetime.date.fromisoformat(s)


def main():
    import argparse
    import textwrap

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        description="calculate revised 30 day retention stats",
        epilog=textwrap.dedent(
            """
            DATE values accept several forms:
              1. ISO 8601 dates ("2021-02-28")
              2. A number of days in the past ("14d")
              3. The literal value "today"
            """
        ),
    )

    parser.add_argument(
        "-s",
        "--since",
        default="7d",
        type=date_or_duration,
        metavar="DATE",
        help="start date (default: %(default)s)",
    )

    parser.add_argument(
        "-u",
        "--until",
        default=datetime.date.today(),
        type=date_or_duration,
        metavar="DATE",
        help="end date (default: today)",
    )

    args = parser.parse_args()

    if args.since >= datetime.date.today():
        parser.error(f"argument -s/--since: must be before today: {args.until}")

    if args.until > datetime.date.today():
        parser.error(f"argument -u/--until: must not be in the future: {args.until}")

    if (args.until - args.since).days < 1:
        parser.error(f"invalid date range: since {args.since} until {args.until}")

    conn = psycopg2.connect(
        dbname=os.environ.get("SYNAPSE_DB_DATABASE", "matrix"),
        user=os.environ.get("SYNAPSE_DB_USERNAME", None),
        password=os.environ.get("SYNAPSE_DB_PASSWORD", None),
        host=os.environ.get("SYNAPSE_DB_HOST", "/tmp"),
        options=os.environ.get("SYNAPSE_DB_OPTIONS", "-c search_path=matrix"),
    )

    conn.set_session(readonly=True, autocommit=True)

    include_clients = args.until > CLIENT_THRESHOLD_DATE

    fieldnames = ["date", "r30"]

    if include_clients:
        fieldnames.extend(sorted(CLIENTS))

    writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
    writer.writeheader()

    for day in range(args.since.toordinal(), args.until.toordinal()):
        date = datetime.date.fromordinal(day)

        r30 = get_r30(conn, date)
        client_r30 = get_r30_by_client(conn, date) if include_clients else {}

        writer.writerow(dict(date=date, r30=r30, **client_r30))

    conn.close()


if __name__ == "__main__":
    main()
