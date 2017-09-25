# -*- coding: utf-8 -*-
from settings import *
from database import connect_db
from datetime import timedelta

if __name__ == '__main__':

    conn_l = connect_db(PG_DATABASE_LOCALHOST)
    cursor_l = conn_l.cursor()
    cursor_l.execute(
        "SELECT OPENID FROM USER_LOCATION GROUP BY OPENID ORDER BY COUNT(OPENID) DESC"
    )
    openid_list = [x[0] for x in cursor_l.fetchall()]
    count_all = len(openid_list)
    counter = 0
    counter_all_delete = 0
    for each_openid in openid_list:
        counter += 1
        cursor_l.execute(
            "SELECT ID, RECORD_TIME FROM USER_LOCATION WHERE OPENID = %s ORDER BY RECORD_TIME", (each_openid,)
        )
        each_openid_data = cursor_l.fetchall()
        need_delete_id = []
        keep_info = each_openid_data[0]
        for each_gps in each_openid_data:
            if each_gps[0] == keep_info[0]:
                continue
            if each_gps[1] - keep_info[1] < timedelta(minutes=20):
                need_delete_id.append(each_gps[0])
            else:
                keep_info = each_gps
        if need_delete_id:
            counter_all_delete += len(need_delete_id)
            cursor_l.execute(
                "DELETE FROM USER_LOCATION WHERE ID in %s", (tuple(need_delete_id),)
            )
            conn_l.commit()
        print u'清理进度：{}/{}, openid={}, 处理数据量={}'.format(counter, count_all, each_openid, len(each_openid_data))

    cursor_l.execute("SELECT COUNT(*) FROM USER_LOCATION")
    rest_data = cursor_l.fetchone()[0]
    conn_l.close()

    print u'清理完成，共删除无效数据 {} 条, 表中剩余有效数据 {} 条'.format(counter_all_delete, rest_data)
