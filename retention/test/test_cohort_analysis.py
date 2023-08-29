import unittest

import retention.cohort_analysis as cohort_analysis
from retention.cohort_analysis import CohortKey


CONFIG = cohort_analysis.Config()

ONE_DAY = 86400_000


def configure_test_db():
    sql_create_users = """
        DROP TABLE IF EXISTS users;
        CREATE TABLE users(
            name text,
            password_hash text,
            creation_ts bigint,
            admin smallint DEFAULT 0 NOT NULL,
            upgrade_ts bigint,
            is_guest smallint DEFAULT 0 NOT NULL,
            appservice_id text,
            consent_version text,
            consent_server_notice_sent text,
            user_type text,
            deactivated smallint DEFAULT 0 NOT NULL,
            shadow_banned boolean
        );"""

    sql_create_user_daily_visits_table = """
        DROP TABLE IF EXISTS user_daily_visits;
        CREATE TABLE user_daily_visits (
            user_id text NOT NULL,
            device_id text,
            "timestamp" bigint NOT NULL,
            user_agent text
        );"""
    sql_create_user_external_ids = """
        DROP TABLE IF EXISTS user_external_ids;
        CREATE TABLE user_external_ids (
            auth_provider text NOT NULL,
            external_id text NOT NULL,
            user_id text NOT NULL
        );"""

    sql_create_devices = """
        DROP TABLE IF EXISTS devices;
        CREATE TABLE devices (
            user_id TEXT NOT NULL,
            device_id TEXT NOT NULL,
            user_agent TEXT NOT NULL
        );
    """

    sql_create_user_ips = """
        DROP TABLE IF EXISTS user_ips;
        CREATE TABLE user_ips (
            user_id TEXT NOT NULL,
            device_id TEXT NOT NULL,
            user_agent TEXT NOT NULL
        );
    """

    with CONFIG.get_conn() as conn:
        # create tables
        if conn is not None:
            # It is necessary to overide the readonly flag for tests, in order
            # to set up the database test conditions. The production script
            # does not require write access.
            conn.set_session(readonly=False)
            create_table(conn, sql_create_users)
            create_table(conn, sql_create_user_daily_visits_table)
            create_table(conn, sql_create_user_external_ids)
            create_table(conn, sql_create_devices)
            create_table(conn, sql_create_user_ips)
        else:
            print("Error! cannot create the database connection.")

    conn.close()


def create_table(conn, create_table_sql):
    """create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Exception as e:
        print(e)


def add_new_user_entry(name, creation_ts):
    """Add a new entry into the users table
    :param name: username
    :param creation_ts: creation ts from the epoch in ms
    :return:
    """
    sql = """
        INSERT INTO users (name, creation_ts)
        VALUES (%(name)s, %(creation_ts)s)
    """

    with CONFIG.get_conn() as conn:
        conn.set_session(readonly=False)
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    sql,
                    {
                        "name": name,
                        "creation_ts": creation_ts / 1000,
                    },
                )
        except Exception as e:
            print(e)
    conn.close()


def add_user_daily_visit_entry(user_id: str, device_id: str, user_agent: str, timestamp: int):
    """
    Adds a new entry into the `user_daily_visits` table.
    """
    sql1 = """
        INSERT INTO user_daily_visits (user_id, device_id, "timestamp", user_agent)
        VALUES (%(user_id)s, %(device_id)s, %(timestamp)s, %(user_agent)s)
        """
    sql2 = """
        INSERT INTO user_ips (user_id, device_id, user_agent)
        VALUES (%(user_id)s, %(device_id)s, %(user_agent)s)
    """

    with CONFIG.get_conn() as conn:
        conn.set_session(readonly=False)
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    sql1,
                    {
                        "user_id": user_id,
                        "device_id": device_id,
                        "timestamp": timestamp,
                        "user_agent": user_agent
                    },
                )
                cursor.execute(
                    sql2,
                    {
                        "user_id": user_id,
                        "device_id": device_id,
                        "user_agent": user_agent
                    },
                )
        except Exception as e:
            print(e)
    conn.close()


class TestCohortAnalysis(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        configure_test_db()

    def test_ts_to_str(self):
        """Test to ensure converting from unix epoch time (ms) to date "%Y-%m-%d"
        works as expected.
        """
        self.assertEqual(cohort_analysis.ts_to_str(1609459200000), "2021-01-01")

    def test_str_to_ts(self):
        """Test to ensure converting from date "%Y-%m-%d" to unix epoch time (ms)
        works as expected.
        """
        self.assertEqual(cohort_analysis.str_to_ts("2021-01-01"), 1609459200000)

    def test_get_new_users(self):
        """Test to ensure that the get_new_users method correctly returns new users
        and respects the timing boundaries.
        """
        date_to_test = 16207776000000
        TWENTY_FOUR_HOURS = 86400000

        add_new_user_entry('user1', date_to_test - TWENTY_FOUR_HOURS)
        add_new_user_entry('user2', date_to_test)
        add_new_user_entry('user3', date_to_test)
        add_new_user_entry('user4', date_to_test + TWENTY_FOUR_HOURS)

        users = cohort_analysis.get_new_users(
            1, date_to_test + (TWENTY_FOUR_HOURS * 10))

        self.assertEqual(len(users), 4)

        users = cohort_analysis.get_new_users(
            date_to_test, date_to_test + TWENTY_FOUR_HOURS)
        self.assertEqual(len(users), 2)

        users = cohort_analysis.get_new_users(
            date_to_test, date_to_test + 2 * TWENTY_FOUR_HOURS)
        self.assertEqual(len(users), 3)

    def test_get_cohort_clients_bucket(self):
        """
        Test the get_cohort_clients_bucket function.
        """
        cohort_start_date = cohort_analysis.str_to_ts("2018-10-01")
        cohort_end_date = cohort_analysis.str_to_ts("2018-10-31")

        # create some users
        add_new_user_entry("user1", cohort_analysis.str_to_ts("2018-10-05"))
        add_new_user_entry("user2", cohort_analysis.str_to_ts("2018-10-07"))
        add_new_user_entry("user3", cohort_analysis.str_to_ts("2018-10-09"))

        # add some visits for the users
        add_user_daily_visit_entry(
            "user1", "U1D1", "Mozilla/5.0", cohort_analysis.str_to_ts("2018-12-13"))
        add_user_daily_visit_entry(
            "user1", "U1D1", "Mozilla/5.0", cohort_analysis.str_to_ts("2018-12-14"))
        add_user_daily_visit_entry(
            "user2", "U2D1", "Riot (iOS; ...)", cohort_analysis.str_to_ts("2018-12-05"))
        add_user_daily_visit_entry(
            "user2", "U2D2", "Element (Android; ...)", cohort_analysis.str_to_ts("2018-12-06"))
        add_user_daily_visit_entry(
            "user2", "U2D3", "Element X (iOS; ...)", cohort_analysis.str_to_ts("2018-12-07"))

        bucket_start_date = cohort_analysis.str_to_ts("2018-12-01")
        bucket_end_date = cohort_analysis.str_to_ts("2018-12-31")

        cohort_users, users_and_devices_to_client = cohort_analysis.get_cohort_users_and_client_mapping(
            cohort_start_date,
            cohort_end_date
        )

        results = list(cohort_analysis.get_cohort_clients_bucket(
            cohort_users,
            cohort_start_date,
            users_and_devices_to_client,
            bucket_start_date,
            bucket_end_date,
        ))

        self.assertEqual(results, [
            (CohortKey("2018-10-01", "android", ""), 1),
            (CohortKey("2018-10-01", "android-riotx", ""), 0),
            (CohortKey("2018-10-01", "electron", ""), 0),
            (CohortKey("2018-10-01", "ios", ""), 1),
            (CohortKey("2018-10-01", "web", ""), 1),
            (CohortKey("2018-10-01", "combined", ""), 2),
            (CohortKey("2018-10-01", "iosx", ""), 1),
            (CohortKey("2018-10-01", "androidx", ""), 0)
        ])

    def test_generate_by_cohort(self):
        """
        Test the generate_by_cohort function.
        THIS TEST CONTAINS A BUG WHICH MATCHES THE CURRENT IMPLEMENTATION.
        """

        cohort_start_date = cohort_analysis.str_to_ts("2018-10-05")

        # create some users
        add_new_user_entry("user1", cohort_analysis.str_to_ts("2018-10-05"))
        add_new_user_entry("user2", cohort_analysis.str_to_ts("2018-10-05"))
        add_new_user_entry("user3", cohort_analysis.str_to_ts("2018-10-05"))

        # add some visits for the users
        add_user_daily_visit_entry(
            "user1", "U1D1", "Mozilla/5.0", cohort_analysis.str_to_ts("2018-10-05"))
        add_user_daily_visit_entry(
            "user1", "U1D1", "Mozilla/5.0", cohort_analysis.str_to_ts("2018-10-06"))
        add_user_daily_visit_entry(
            "user1", "U1D1", "Mozilla/5.0", cohort_analysis.str_to_ts("2018-10-07"))
        add_user_daily_visit_entry(
            "user1", "U1D2", "Element X (iOS; ...)", cohort_analysis.str_to_ts("2018-10-07"))
        add_user_daily_visit_entry(
            "user2", "U2D1", "Riot (iOS; ...)", cohort_analysis.str_to_ts("2018-10-05"))
        add_user_daily_visit_entry(
            "user2", "U2D1", "Riot (iOS; ...)", cohort_analysis.str_to_ts("2018-10-06"))
        add_user_daily_visit_entry(
            "user2", "U2D2", "Element (Android; ...)", cohort_analysis.str_to_ts("2018-10-05"))
        add_user_daily_visit_entry(
            "user2", "U2D2", "Element (Android; ...)", cohort_analysis.str_to_ts("2018-10-06"))
        add_user_daily_visit_entry(
            "user3", "U3D1", "Element (Android; ...)", cohort_analysis.str_to_ts("2018-10-05"))
        add_user_daily_visit_entry(
            "user3", "U3D1", "Element (Android; ...)", cohort_analysis.str_to_ts("2018-10-06"))
        add_user_daily_visit_entry(
            "user3", "U3D2", "Element X (Android; ...)", cohort_analysis.str_to_ts("2018-10-06"))

        results = list(cohort_analysis.generate_by_cohort(
            cohort_start_date,
            buckets=7,
            period=ONE_DAY,
        ))

        self.assertEqual(results, [
            (CohortKey(cohort_start_date='2018-10-05',
             client_type='android', sso_idp=''), 1, 2, None),
            (CohortKey(cohort_start_date='2018-10-05',
             client_type='android-riotx', sso_idp=''), 1, 0, None),
            (CohortKey(cohort_start_date='2018-10-05',
             client_type='electron', sso_idp=''), 1, 0, None),
            (CohortKey(cohort_start_date='2018-10-05',
             client_type='ios', sso_idp=''), 1, 1, None),
            (CohortKey(cohort_start_date='2018-10-05',
             client_type='web', sso_idp=''), 1, 1, None),
            (CohortKey(cohort_start_date='2018-10-05',
             client_type='combined', sso_idp=''), 1, 3, 3),
            (CohortKey(cohort_start_date='2018-10-05',
             client_type='iosx', sso_idp=''), 1, 0, None),
            (CohortKey(cohort_start_date='2018-10-05',
             client_type='androidx', sso_idp=''), 1, 0, None),
            (CohortKey(cohort_start_date='2018-10-05',
             client_type='android', sso_idp=''), 2, 2, None),
            (CohortKey(cohort_start_date='2018-10-05',
             client_type='android-riotx', sso_idp=''), 2, 0, None),
            (CohortKey(cohort_start_date='2018-10-05',
             client_type='electron', sso_idp=''), 2, 0, None),
            (CohortKey(cohort_start_date='2018-10-05',
             client_type='ios', sso_idp=''), 2, 1, None),
            (CohortKey(cohort_start_date='2018-10-05',
             client_type='web', sso_idp=''), 2, 1, None),
            (CohortKey(cohort_start_date='2018-10-05',
             client_type='combined', sso_idp=''), 2, 3, 3),
            (CohortKey(cohort_start_date='2018-10-05',
             client_type='iosx', sso_idp=''), 2, 0, None),
            (CohortKey(cohort_start_date='2018-10-05',
             client_type='androidx', sso_idp=''), 2, 1, None),
            (CohortKey(cohort_start_date='2018-10-05',
             client_type='android', sso_idp=''), 3, 0, None),
            (CohortKey(cohort_start_date='2018-10-05',
             client_type='android-riotx', sso_idp=''), 3, 0, None),
            (CohortKey(cohort_start_date='2018-10-05',
             client_type='electron', sso_idp=''), 3, 0, None),
            (CohortKey(cohort_start_date='2018-10-05',
             client_type='ios', sso_idp=''), 3, 0, None),
            (CohortKey(cohort_start_date='2018-10-05',
             client_type='web', sso_idp=''), 3, 1, None),
            (CohortKey(cohort_start_date='2018-10-05',
             client_type='combined', sso_idp=''), 3, 1, 3),
            (CohortKey(cohort_start_date='2018-10-05',
             client_type='iosx', sso_idp=''), 3, 1, None),
            (CohortKey(cohort_start_date='2018-10-05',
             client_type='androidx', sso_idp=''), 3, 0, None)
        ])

    def test_generate_by_bucket(self):
        """
        Test the generate_by_bucket function.
        THIS TEST CONTAINS A BUG WHICH MATCHES THE CURRENT IMPLEMENTATION.
        """

        bucket_start_date = cohort_analysis.str_to_ts("2018-10-06")

        # create some users
        add_new_user_entry("user1", cohort_analysis.str_to_ts("2018-10-05"))
        add_new_user_entry("user2", cohort_analysis.str_to_ts("2018-10-05"))
        add_new_user_entry("user3", cohort_analysis.str_to_ts("2018-10-05"))
        add_new_user_entry("user4", cohort_analysis.str_to_ts("2018-10-06"))
        add_new_user_entry("user5", cohort_analysis.str_to_ts("2018-10-06"))

        # add some visits for the users
        add_user_daily_visit_entry(
            "user1", "U1D1", "Mozilla/5.0", cohort_analysis.str_to_ts("2018-10-05"))
        add_user_daily_visit_entry(
            "user1", "U1D1", "Mozilla/5.0", cohort_analysis.str_to_ts("2018-10-06"))
        add_user_daily_visit_entry(
            "user1", "U1D1", "Mozilla/5.0", cohort_analysis.str_to_ts("2018-10-07"))
        add_user_daily_visit_entry(
            "user2", "U2D1", "Riot (iOS; ...)", cohort_analysis.str_to_ts("2018-10-05"))
        add_user_daily_visit_entry(
            "user2", "U2D2", "Element X (iOS; ...)", cohort_analysis.str_to_ts("2018-10-05"))
        add_user_daily_visit_entry(
            "user2", "U2D1", "Riot (iOS; ...)", cohort_analysis.str_to_ts("2018-10-06"))
        add_user_daily_visit_entry(
            "user2", "U2D2", "Element (Android; ...)", cohort_analysis.str_to_ts("2018-10-05"))
        add_user_daily_visit_entry(
            "user2", "U2D2", "Element (Android; ...)", cohort_analysis.str_to_ts("2018-10-06"))
        add_user_daily_visit_entry(
            "user3", "U3D1", "Element (Android; ...)", cohort_analysis.str_to_ts("2018-10-06"))
        add_user_daily_visit_entry(
            "user3", "U3D1", "Element (Android; ...)", cohort_analysis.str_to_ts("2018-10-07"))
        add_user_daily_visit_entry(
            "user4", "U4D1", "Element (Android; ...)", cohort_analysis.str_to_ts("2018-10-06"))
        add_user_daily_visit_entry(
            "user4", "U4D1", "Element (Android; ...)", cohort_analysis.str_to_ts("2018-10-07"))
        add_user_daily_visit_entry(
            "user5", "U5D1", "Element (Android; ...)", cohort_analysis.str_to_ts("2018-10-06"))
        add_user_daily_visit_entry(
            "user5", "U5D2", "Element X (iOS; ...)", cohort_analysis.str_to_ts("2018-10-06"))

        results = list(cohort_analysis.generate_by_bucket(
            bucket_start_date,
            buckets=7,
            period=ONE_DAY,
        ))

        self.assertEqual(results, [

            (CohortKey(cohort_start_date='2018-10-06',
             client_type='android', sso_idp=''), 1, 2, None),
            (CohortKey(cohort_start_date='2018-10-06',
             client_type='android-riotx', sso_idp=''), 1, 0, None),
            (CohortKey(cohort_start_date='2018-10-06',
             client_type='electron', sso_idp=''), 1, 0, None),
            (CohortKey(cohort_start_date='2018-10-06',
             client_type='ios', sso_idp=''), 1, 0, None),
            (CohortKey(cohort_start_date='2018-10-06',
             client_type='web', sso_idp=''), 1, 0, None),
            (CohortKey(cohort_start_date='2018-10-06',
             client_type='combined', sso_idp=''), 1, 2, 2),
            (CohortKey(cohort_start_date='2018-10-06',
                       client_type='iosx', sso_idp=''), 1, 1, None),
            (CohortKey(cohort_start_date='2018-10-06',
             client_type='androidx', sso_idp=''), 1, 0, None),
            (CohortKey(cohort_start_date='2018-10-05',
             client_type='android', sso_idp=''), 2, 2, None),
            (CohortKey(cohort_start_date='2018-10-05',
             client_type='android-riotx', sso_idp=''), 2, 0, None),
            (CohortKey(cohort_start_date='2018-10-05',
             client_type='electron', sso_idp=''), 2, 0, None),
            (CohortKey(cohort_start_date='2018-10-05',
             client_type='ios', sso_idp=''), 2, 1, None),
            (CohortKey(cohort_start_date='2018-10-05',
             client_type='web', sso_idp=''), 2, 1, None),
            (CohortKey(cohort_start_date='2018-10-05',
             client_type='combined', sso_idp=''), 2, 3, 3),
            (CohortKey(cohort_start_date='2018-10-05',
             client_type='iosx', sso_idp=''), 2, 0, None),
            (CohortKey(cohort_start_date='2018-10-05',
                       client_type='androidx', sso_idp=''), 2, 0, None),
        ])

    def test_user_agent_to_client(self):
        test_ua = [
            # Electron
            ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Element/1.11.33 Chrome/112.0.5615.183 Electron/24.3.1 Safari/537.36",
             cohort_analysis.ELEMENT_ELECTRON),
            (" Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Element/1.11.31 Chrome/112.0.5615.87 Electron/24.1.2 Safari/537.36",
             cohort_analysis.ELEMENT_ELECTRON),

            # Web
            ("Mozilla/5.0 (Windows NT 10.0; rv:102.0) Gecko/20100101 Firefox/102.0",
             cohort_analysis.WEB),
            ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36", cohort_analysis.WEB),

            # Android
            ("Element/1.5.25 (HUAWEI MOA-LX9N; Android 10; MOA-L49 3.1.0.110(C10E3R3P1); Flavour FDroid; MatrixAndroidSdk2 1.5.25)",
                cohort_analysis.ELEMENT_ANDROID),
            ("Element/1.5.32 (Google Pixel 6 Pro; Android 13; TQ2A.230505.002; Flavour GooglePlay; MatrixAndroidSdk2 1.5.32)",
                cohort_analysis.ELEMENT_ANDROID),

            # iOS
            ("Element/1.10.12 (iPhone 14 Pro Max; iOS 16.4.1; Scale/3.00)",
                cohort_analysis.ELEMENT_IOS),
            ("Element/1.10.12 (iPhone 7 Plus; iOS 15.7.3; Scale/1.00)",
                cohort_analysis.ELEMENT_IOS),

            # Element X Android - TODO replace with real UA
            ("Element X/0.1.0 (Google Pixel 4a (5G); Android 13; TQ3A.230605.011.2023062800; Sdk TODO)",
             cohort_analysis.ELEMENTX_ANDROID),

            # Element X iOS
            ("Element X/1.1.5 (iPhone 11; iOS 16.5; Scale/2.00)", cohort_analysis.ELEMENTX_IOS)
        ]
        for i in test_ua:
            self.assertEqual(i[1], cohort_analysis.user_agent_to_client(i[0]))


if __name__ == "__main__":
    unittest.main()
