import unittest
import retention.cohort_analysis as cohort_analysis

CONFIG = cohort_analysis.Config()


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


class TestCohortAnalysis(unittest.TestCase):
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

        users = cohort_analysis.get_new_users(1, date_to_test + (TWENTY_FOUR_HOURS * 10))

        self.assertEqual(len(users), 4)

        users = cohort_analysis.get_new_users(date_to_test, date_to_test + TWENTY_FOUR_HOURS)
        self.assertEqual(len(users), 2)

        users = cohort_analysis.get_new_users(date_to_test, date_to_test + 2 * TWENTY_FOUR_HOURS)
        self.assertEqual(len(users), 3)


if __name__ == "__main__":
    unittest.main()