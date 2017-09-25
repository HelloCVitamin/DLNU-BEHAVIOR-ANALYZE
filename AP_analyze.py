# -*- coding: utf-8 -*-
from settings import *
from database import connect_db

if __name__ == '__main__':

    conn_l = connect_db(PG_DATABASE_LOCALHOST)
    cursor_l = conn_l.cursor()
    cursor_l.execute(
        "SELECT SOURCE_IP, count(SOURCE_IP) "
        "FROM USER_LOGIN_HISTORY "
        "GROUP BY SOURCE_IP "
        "ORDER BY COUNT(SOURCE_IP) DESC "
    )
    ap_list = [x[0] for x in cursor_l.fetchall()]

    count_ap, count_1 = len(ap_list), 0

    already_insert = cursor_l.execute("SELECT SOURCE_IP FROM AP_GPS_PINS GROUP BY SOURCE_IP")
    already_insert_list = [x[0] for x in cursor_l.fetchall()]

    user_openid_dict = {}

    for each_ in ap_list:
        count_1 += 1
        if each_ in already_insert_list:
            print u'AP进度：{}/{}'.format(count_1, count_ap)
            continue
        ap_gps = []
        cursor_l.execute(
            "SELECT UID, LOGIN_TIME, LOGOUT_TIME "
            "FROM user_login_history "
            "WHERE source_ip = %(ap_ip)s",
            {'ap_ip': each_}
        )
        login_info_list = cursor_l.fetchall()

        count_2 = 0
        for each__ in login_info_list:
            count_2 += 1
            openid_list = user_openid_dict.get(each__[0])
            if not openid_list:
                cursor_l.execute("SELECT openid from user_openid where uid = %(uid)s", {'uid': each__[0]})
                openid_list = tuple(x[0] for x in cursor_l.fetchall())
                user_openid_dict[each__[0]] = openid_list

            if openid_list:
                cursor_l.execute(
                    "SELECT longitude, latitude, record_time "
                    "from user_location "
                    "where openid IN %(openid_list)s "
                    "and record_time between %(begin_time)s and %(end_time)s",
                    {
                        'openid_list': openid_list,
                        'begin_time': each__[1].strftime("%Y-%m-%d %H:%M:%S"),
                        'end_time': each__[2].strftime("%Y-%m-%d %H:%M:%S")
                    }
                )
                ap_gps += cursor_l.fetchall()
                print u'当前AP记录查询进度：{}/{}  AP进度：{}/{}'.format(count_2, len(login_info_list), count_1, count_ap)
        if ap_gps:
            for each_gps in ap_gps:
                cursor_l.execute(
                    "INSERT INTO AP_GPS_PINS(SOURCE_IP, LONGITUDE, LATITUDE, RECORD_TIME) "
                    "VALUES(%(ip)s, %(lo)s, %(la)s, %(time)s)",
                    {'ip': each_, 'lo': each_gps[0], 'la': each_gps[1], 'time': each_gps[2]}
                )
        else:
            cursor_l.execute(
                "INSERT INTO AP_GPS_PINS(SOURCE_IP, LONGITUDE, LATITUDE, RECORD_TIME) "
                "VALUES(%(ip)s, %(lo)s, %(la)s, %(time)s)",
                {'ip': each_, 'lo': 0.0, 'la': 0.0, 'time': '2020-01-01 00:00:00'}
            )
        conn_l.commit()
    conn_l.close()
