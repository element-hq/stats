from psycopg2 import connect, errors
import datetime
import time
import calendar
import pytz


def get_db_conn():
    try:
        conn = connect(
            dbname="matrix",
            user="readonly",
            host="localhost",
            password="",
            options="-c search_path=matrix",
            port=5433
        )
        return conn
    except (Exception) as error:
        print("Can't connect to db :( %s", error)

# Replaced by improved query ensuring MAX(Timestamp)
# def get_rX_usernames(r, day):
#     """
#     Takes a conn and a datetime specifying the day to analyse. Must start at midnight UTC.
#     that form that r30 grouping. Returns a tuple of usernames
#     """
#     sql = """
#         SELECT users.name from users
#         JOIN user_daily_visits ON users.name=user_daily_visits.user_id
#         WHERE users.name IN (
#                 select user_id
#                 from user_daily_visits
#                 where timestamp <= %s
#                 AND timestamp > 1000 * ((%s /1000)  - (%s * 86400))
#         )
#         AND user_daily_visits.timestamp/1000 - users.creation_ts > 86400 * %s
#         AND appservice_id IS NULL AND is_guest=0 GROUP BY users.name
# 	"""

#     unix_day = int(time.mktime(day.timetuple()) * 1000)
#     begin = time.time()
#     with get_db_conn() as conn:
#         with conn.cursor() as cursor:
#             cursor.execute(sql, (unix_day, unix_day, r, r))
#             raw_results = cursor.fetchall()
#     conn.close()

#     pause = time.time() - begin
#     time.sleep(pause)
#     results_set = set()
#     for r in raw_results:
#         results_set.add(r[0])
#     return results_set


def get_rX_usernames(r, day):
    """
    Takes a conn and a datetime specifying the day to analyse. Must start at midnight UTC.
    that form that r30 grouping. Returns a tuple of usernames
    """
    sql = """
        SELECT users.name from users
        JOIN user_daily_visits ON users.name=user_daily_visits.user_id
        WHERE users.name IN (
                select user_id
                from user_daily_visits
                where timestamp <= %s
                AND timestamp > 1000 * ((%s /1000)  - (%s * 86400))
        )
        AND appservice_id IS NULL AND is_guest=0 GROUP BY users.name
        HAVING MAX(user_daily_visits.timestamp/1000) - MAX(users.creation_ts) > 86400 * %s
        AND MAX(user_daily_visits.timestamp) <= %s
	"""
    unix_day = int(time.mktime(day.timetuple()) * 1000)
    begin = time.time()
    with get_db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, (unix_day, unix_day, r, r, unix_day))
            raw_results = cursor.fetchall()
    conn.close()

    pause = time.time() - begin
    time.sleep(pause)
    results_set = set()
    for r in raw_results:
        results_set.add(r[0])
    return results_set


def get_rX_usernames_new(r, day):
    """
    r (int) period to retain over
    day (datetime) Specifying the day to analyse. Must start at midnight UTC.
    Returns a tuple of usernames
    """

    sql = """
        SELECT user_id from user_daily_visits
        WHERE timestamp > %s - (cast(1 as bigint) * 86400 * 2 * %s * 1000)
        AND timestamp <= %s
        GROUP BY user_id
        HAVING ( max(timestamp) - min(timestamp) > (cast(1 as bigint) * 86400 * %s * 1000));
    """

    unix_day = int(time.mktime(day.timetuple()) * 1000)
    begin = time.time()
    with get_db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, (unix_day, r, unix_day, r))
            raw_results = cursor.fetchall()
    conn.close()

    pause = time.time() - begin
    time.sleep(pause)
    results_set = set()
    for r in raw_results:
        results_set.add(r[0])
    return results_set


def get_users_between_dates(start, stop):
    """

    Args:
        start ([datetime]): [period to start from]
        stop ([type]): [period to stop from]
    """

    sql = """
    SELECT user_id from user_daily_visits
    WHERE timestamp > %s
    AND timestamp <= %s
    """
    start_unix = int(calendar.timegm(start.timetuple()) * 1000)

    stop_unix = int(calendar.timegm(stop.timetuple()) * 1000)
    begin = time.time()
    with get_db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, (start_unix, stop_unix))
            raw_results = cursor.fetchall()
    conn.close()

    pause = time.time() - begin
    time.sleep(pause)
    results_set = set()
    for r in raw_results:
        results_set.add(r[0])
    return results_set


def four_week_actives(day):
    end_day = day
    weeks = []
    for x in range(0, 4):
        start_day = end_day - datetime.timedelta(days=7)
        weeks.append(get_users_between_dates(start_day, end_day))
        end_day = start_day
    return weeks[0] & weeks[1] & weeks[2] & weeks[3]


def plot_rX_over_time(r):
    # start of time
    day = datetime.datetime(2018, 7, 1)
    #day = datetime.datetime.utcfromtimestamp(1530403200000)

    #25th Sept 2020
    day = datetime.datetime.utcfromtimestamp(1600992000)
    now = datetime.datetime.utcnow()
    rX_scores = []
    while day < now:

        users_new = get_rX_usernames_new(r, day)
        users = get_rX_usernames(r, day)
        rX_scores.append((day, len(users_new), len(users)))
        print(day, len(users_new), len(users))
        day += datetime.timedelta(days=1)
    print(rX_scores)
    print("%s,%s,%s" % (rX_scores[0], rX_scores[1],  rX_scores[2]))


def plot_four_week_actives_over_time():
    # start of time
    day = datetime.datetime(2018, 7, 1)
    day = datetime.datetime.utcfromtimestamp(1530403200)
    now = datetime.datetime.utcnow()
    four_week_totals = []
    while day < now:

        users = four_week_actives(day)

        four_week_totals.append((day, len(users)))
        print(day, len(users))
        day += datetime.timedelta(days=1)


def plot_monthly_active_users():
    # start of time
    month = datetime.datetime.utcfromtimestamp(1530403200)
    now = datetime.datetime.utcnow()
    monthly_active_totals = []
    while month < now:

        users = get_users_between_dates(month - datetime.timedelta(days=30), month)

        monthly_active_totals.append((month, len(users)))
        print(month, len(users))
        month += datetime.timedelta(days=7)


def get_user_creation_histogram(users):
    """
    Given a Set of users, determine when they originally created their account
    """
    sql = """
        SELECT count(*), date_trunc('month', to_timestamp(creation_ts)) as month
        FROM users where users.name in %s
        GROUP BY month ORDER BY month
        """
    begin = time.time()
    with get_db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, (tuple(users),))
            raw_result = cursor.fetchall()
    conn.close()

    # Running this query on secondary database not tuned for long running queries
    # this is really heavy handed way of allowing background processes to keep up :/
    pause = time.time() - begin
    time.sleep(pause)
    hist = []
    for r in raw_result:
        hist.append((r[1].strftime("%Y-%m"), r[0]))
    return hist


def get_user_creation_times(users):
    """Given a set of users, determine their specific creation times
    """
    sql = """
        SELECT name, date_trunc('day', to_timestamp(creation_ts)) as day
        FROM users where users.name in %s
        ORDER by day
        """
    with get_db_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, (tuple(users),))
            raw_result = cursor.fetchall()
    conn.close()
    users = []
    for r in raw_result:
        users.append((r[1].strftime("%D"), r[0]))
    return users


def get_utc_datetime(date_string):
    """ Takes date string in form "2018-07-01" and returns datetime in UTC"""
    import pytz

    unaware = datetime.datetime.strptime(date_string, "%Y-%m-%d")
    return pytz.utc.localize(unaware)


def get_churn_new_entries_over_time(start_date, r):
    current_date = start_date
    now = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
    yesterday = start_date - datetime.timedelta(days=1)
    results = []
    churn_histograms = []
    y = get_rX_usernames( r, yesterday)
    while current_date < now:
        try:
            # t is today_set, y is yesterday set
            t = get_rX_usernames(r, current_date)
            # date, total for today, intersection, churn, new entries
            results.append(
                (
                    current_date.strftime("%Y %m %d"),
                    len(t),
                    len(t & y),
                    len(y - t),
                    len(t - y),
                )
            )
            # New
            hist = get_user_creation_histogram(y-t)
            churn_histograms.append(
                (
                    current_date.strftime("%Y %m %d"),
                    hist
                )
            )
            ####
        except psycopg2.errors.SerializationFailure as e:
            print(e)
            time.time.sleep(60)
            continue
        y = t
        yesterday = current_date
        current_date = current_date + datetime.timedelta(days=1)
    return results, churn_histograms
