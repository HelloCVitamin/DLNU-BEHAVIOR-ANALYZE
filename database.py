# -*- coding: utf-8 -*-
import psycopg2
from settings import *
from decript import decrypt


def connect_db(connect_info):
    conn = psycopg2.connect(**connect_info)
    return conn


def account_list():
    conn_l = connect_db(PG_DATABASE_LOCALHOST)
    cursor_l = conn_l.cursor()
    cursor_l.execute("SELECT UID, PASSWORD, ID FROM UID_PASSWORD ORDER BY random()")
    accounts = cursor_l.fetchall()
    conn_l.close()

    already_inserted_uid = inserted_uid()
    return [x for x in accounts if x[0] not in already_inserted_uid]


def inserted_uid():
    conn_l = connect_db(PG_DATABASE_LOCALHOST)
    cursor_l = conn_l.cursor()
    cursor_l.execute("SELECT UID FROM USER_LOGIN_HISTORY GROUP BY UID")
    uid_list = cursor_l.fetchall()
    conn_l.close()
    return [x[0] for x in uid_list]


def input_accounts_to_local():
    conn_r = connect_db(PG_DATABASE_REMOTE)
    cursor_r = conn_r.cursor()
    cursor_r.execute(
        "SELECT owner_id, account, PASSWORD FROM user_accounts "
        "WHERE sys_id = 2 AND account ILIKE '20%' ORDER BY account"
    )
    all_account = cursor_r.fetchall()
    conn_r.close()

    conn_l = connect_db(PG_DATABASE_LOCALHOST)
    cursor_l = conn_l.cursor()
    cursor_l.execute("DELETE FROM UID_PASSWORD")
    for each_ in all_account:
        cursor_l.execute(
            "INSERT INTO UID_PASSWORD (UID, PASSWORD) VALUES (%(uid)s, %(pwd)s)",
            {'uid': each_[1], 'pwd': decrypt(each_[0], each_[2])}
        )
        print each_[1]
    conn_l.commit()
    conn_l.close()


if __name__ == '__main__':
    input_accounts_to_local()
