# get_ipython().getoutput("pip install elasticsearch")
# get_ipython().getoutput("pip install pyecharts ")
# get_ipython().getoutput("pip install pandas")
# get_ipython().getoutput("pip install pymysql")
# get_ipython().getoutput("pip install sqlalchemy ")
# get_ipython().getoutput("pip install Elasticsearch")
# get_ipython().getoutput("pip install matplotlib")


# 导出html 不显示代码
from IPython.display import HTML
HTML('''<script>  
code_show=true; 
function code_toggle() {undefined
  if (code_show){undefined
    $(\'div.input\').hide();
  } else {undefined
    $(\'div.input\').show();
  }
  code_show = get_ipython().getoutput("code_show")
}  
$( document ).ready(code_toggle);
</script>
  <form action="javascript:code_toggle()">
    <input type="submit" value="Click here to toggle on/off the raw code.">
 </form>''')



from pyecharts.globals import CurrentConfig, NotebookType
CurrentConfig.NOTEBOOK_TYPE = NotebookType.JUPYTER_LAB



import pandas as pd
import pymysql
import numpy as np
from sqlalchemy import create_engine
from elasticsearch import Elasticsearch
import datetime
import os
from functools import reduce
from matplotlib import pyplot as plt 
import json


from pyecharts import options as opts
from pyecharts.charts import Line,Bar,Pie


#连接es时host只写ip
es_hosts = str("52.82.47.234,52.83.95.66").split(",")
es = Elasticsearch(es_hosts)

#链接mysql
def mysqlDB(sql):
    engine = create_engine(
        'mysql+pymysql://schu:slavep@123.103.75.152:3306/school')
    result = pd.read_sql_query(sql = sql, con = engine)
    return result

#链接sqlite
def sqliteDB(sql):
    engine = create_engine('sqlite:///F:\\PythonObject\\SchoolObject\\db.sqlite3') 
    result = pd.read_sql(sql = sql, con = engine)
    return result


get_ipython().run_cell_magic("HTML", "", """<style type="text/css">
table.dataframe td, table.dataframe th {
    border: 1px  black solid !important;
  color: black !important;
}""")


# 显示所有行
pd.set_option("display.max_rows", None)
# 显示所有列
pd.set_option("display.max_columns", None)
# 保留小数点后几位
pd.set_option('precision', 0)
#取消科学计数法
pd.set_option("display.float_format", lambda x: "get_ipython().run_line_magic(".1f"", " % x)")

now_time = datetime.datetime.strftime(datetime.datetime.today(),'get_ipython().run_line_magic("Y%m%d')", "")



import sqlite3

con = sqlite3.connect('F:\\PythonObject\\SchoolObject\\db.sqlite3')


def mysqlSqlite(sql, db_name):
    
    mysqlDB(sql = sql).to_sql(name=db_name, con=con,if_exists='replace', index=False)

    return '同步成功'





# school_course_task
school_course_task_sql = '''
        SELECT fr.school_id,fr.name,tc.course_id,tc.course_name ,g.grade_name,s.subject_name ,tt.task_id,tt.task_full_name,tt.c_user_id ,t.teacher_name ,tt.task_type ,tt.c_time,tt.correct_model,tt.classroom_id
        FROM tp_task_info tt ,tp_course_info tc,franchised_school_info fr,teacher_info t,tp_j_course_class tjc,subject_info s,grade_info g 
        WHERE tc.DC_SCHOOL_ID = fr.school_id  and tc.course_id = tt.COURSE_ID  and fr.school_id >50000 and fr.enable = 0 and tt.c_time  >'2021-1-15 0:00:00'  and tc.LOCAL_STATUS = 1 
        and fr.school_type in (3,4) and tt.C_USER_ID = t.user_id and tjc.course_id = tc.course_id  and tjc.subject_id = s.subject_id and tjc.grade_id = g.grade_id 
        '''
mysqlSqlite(sql=school_course_task_sql, db_name='school_course_task')


# # j_task_class
# task_class_sql = '''
#    SELECT task_id ,user_type_id as task_class_count FROM tp_task_allot_info where user_type = 0   and c_time  >'2020-7-15 0:00:00'

# '''
# mysqlSqlite(sql=task_class_sql, db_name='j_task_class')


# # j_task_group
# task_group_sql = '''
#     SELECT tta.task_id ,tg.class_id   
#     FROM tp_task_allot_info tta LEFT JOIN  tp_group_info tg on  tg.group_id = tta.user_type_id 
#     where tta.user_type = 2  
#     and tta.c_time  >'2020-7-15 0:00:00'

# '''
# mysqlSqlite(sql=task_group_sql, db_name='j_task_group')






#课件
word_grade_subject_sql = ('''
        SELECT g.grade_name,s.subject_name,COUNT(DISTINCT r.res_id) "word_count"
        FROM 
        rs_resource_info r,tp_j_course_resource_info tr,
        tp_course_info tc,`tp_j_course_teaching_material` ttm
        ,`teaching_material_info` tm
        ,`teach_version_info` tv
        ,grade_info g
        ,subject_info s
        WHERE tr.`course_id` = ttm.`course_id`
        AND r.res_id  = tr.`res_id`
        AND tc.`COURSE_ID` > 0 
        AND tc.`COURSE_ID` = tr.`course_id`
        AND ttm.`teaching_material_id` = tm.`material_id` AND tm.`version_id` =  tv.`version_id`
        AND tm.`grade_id` = g.`GRADE_ID` AND tm.`subject_id` = s.`SUBJECT_ID`
        AND tc.`LOCAL_STATUS` = 1
        AND r.`diff_type` = 0
        AND r.res_id > 0 
        AND r.`RES_STATUS` = 1
        GROUP BY g.grade_id,s.subject_id
        ORDER BY g.grade_id,s.subject_id
             ''')
word_grade_subject_count = mysqlDB(word_grade_subject_sql)

#关联学案的微课
mp4_word_grade_subject_sql = ('''
        SELECT g.grade_name,s.subject_name,COUNT(DISTINCT r.res_id) as 'mp4_word_count'
        FROM 
        rs_resource_info r,tp_j_course_resource_info tr,
        tp_course_info tc,`tp_j_course_teaching_material` ttm
        ,`teaching_material_info` tm
        ,`teach_version_info` tv
        ,grade_info g
        ,subject_info s
        ,tp_study_j_module_resource aaa
        , j_mic_video_paper bbb
        WHERE tr.`course_id` = ttm.`course_id`
        AND r.res_id  = tr.`res_id`
        AND tc.`COURSE_ID` > 0 
        AND tc.`COURSE_ID` = tr.`course_id`
        AND ttm.`teaching_material_id` = tm.`material_id` AND tm.`version_id` =  tv.`version_id`
        AND tm.`grade_id` = g.`GRADE_ID` AND tm.`subject_id` = s.`SUBJECT_ID`
        AND tc.`LOCAL_STATUS` = 1
        AND aaa.resource_id = r.res_id
        AND aaa.resource_type = 32
        AND r.`diff_type` = 1
        AND r.res_id > 0 
        AND r.`RES_STATUS` = 1
        AND bbb.`mic_video_id` = r.res_id
        AND EXISTS(
          SELECT 1 FROM tp_study_j_module_resource ppp
           WHERE aaa.module_id=ppp.module_id
        AND  ppp.resource_type=33
        )
        GROUP BY g.grade_id,s.subject_id
        ORDER BY g.grade_id,s.subject_id
             ''')
mp4_word_grade_subject_count = mysqlDB(mp4_word_grade_subject_sql)

#微课关联的学案
word_mp4_grade_subject_sql = ('''
        SELECT g.grade_name,s.subject_name, COUNT(DISTINCT r.res_id) as 'word_mp4_count'
        FROM 
        rs_resource_info r,tp_j_course_resource_info tr,
        tp_course_info tc,`tp_j_course_teaching_material` ttm
        ,`teaching_material_info` tm
        ,`teach_version_info` tv
        ,grade_info g
        ,subject_info s
        ,tp_study_j_module_resource aaa
        WHERE tr.`course_id` = ttm.`course_id`
        AND r.res_id  = tr.`res_id`
        AND tc.`COURSE_ID` > 0 
        AND tc.`COURSE_ID` = tr.`course_id`
        AND ttm.`teaching_material_id` = tm.`material_id` AND tm.`version_id` =  tv.`version_id`
        AND tm.`grade_id` = g.`GRADE_ID` AND tm.`subject_id` = s.`SUBJECT_ID`
        AND tc.`LOCAL_STATUS` = 1
        AND aaa.resource_id = r.res_id
        AND aaa.resource_type = 33
        AND r.`diff_type` = 0
        AND r.res_id > 0 
        AND r.`RES_STATUS` = 1
        AND EXISTS(
          SELECT 1 FROM tp_study_j_module_resource ppp
           WHERE aaa.module_id=ppp.module_id
        AND  ppp.resource_type=32
        )
        GROUP BY g.grade_id,s.subject_id
        ORDER BY g.grade_id,s.subject_id
                     ''')
word_mp4_grade_subject_count = mysqlDB(word_mp4_grade_subject_sql)

#微课
mp4_grade_subject_sql = ('''
        SELECT g.grade_name,s.subject_name,COUNT(DISTINCT r.res_id) "mp4_count"
        FROM 
        rs_resource_info r,tp_j_course_resource_info tr,
        tp_course_info tc,`tp_j_course_teaching_material` ttm
        ,`teaching_material_info` tm
        ,`teach_version_info` tv
        ,grade_info g
        ,subject_info s
        WHERE tr.`course_id` = ttm.`course_id`
        AND r.res_id  = tr.`res_id`
        AND tc.`COURSE_ID` > 0 
        AND tc.`COURSE_ID` = tr.`course_id`
        AND ttm.`teaching_material_id` = tm.`material_id` AND tm.`version_id` =  tv.`version_id`
        AND tm.`grade_id` = g.`GRADE_ID` AND tm.`subject_id` = s.`SUBJECT_ID`
        AND tc.`LOCAL_STATUS` = 1
        AND r.`diff_type` = 1
        AND r.res_id > 0 
        AND r.`RES_STATUS` = 1
        GROUP BY g.grade_id,s.subject_id
        ORDER BY g.grade_id,s.subject_id
             ''')
mp4_grade_subject_count = mysqlDB(mp4_grade_subject_sql)

#成卷
cpaper_grade_subject_sql = ('''
        SELECT g.grade_name,s.subject_name,COUNT(DISTINCT p.paper_id) "cpaper_count" FROM `online_test_paper_info` p,subject_info s,grade_info g 
        WHERE paper_id > 0
        AND g.grade_id = p.grade_id
        AND s.subject_id= p.subject_id
        AND paper_status = 1
        GROUP BY g.grade_id,s.subject_id
        ORDER BY g.grade_id,s.subject_id
             ''')
cpaper_grade_subject_count = mysqlDB(cpaper_grade_subject_sql)
    
#AB卷
paper_grade_subject_sql = ('''
        SELECT g.grade_name,s.subject_name,COUNT(DISTINCT p.paper_id) "paper_count"
        FROM paper_info p
        ,tp_j_course_paper tr
        ,tp_course_info tc
        ,tp_j_course_teaching_material ttm
        ,teach_version_info tv
        ,teaching_material_info tm
        ,grade_info g
        ,subject_info s
        WHERE tr.`course_id` = ttm.`course_id`
        AND p.paper_id = tr.`paper_id`
        AND tc.`COURSE_ID` > 0
        AND tc.`COURSE_ID` = tr.`course_id`
        AND ttm.`teaching_material_id` = tm.`material_id`
        AND tm.`version_id` = tv.`version_id`
        AND tm.`grade_id` = g.`GRADE_ID`
        AND tm.`subject_id` = s.`SUBJECT_ID`
        AND tc.`LOCAL_STATUS` = 1
        AND p.paper_id > 0 
        AND p.`paper_type` IN (1,2)
        GROUP BY g.grade_id,s.subject_id
        ORDER BY g.grade_id,s.subject_id
             ''')
paper_grade_subject_count = mysqlDB(paper_grade_subject_sql)

#试题
ques_grade_subject_sql = ('''
        SELECT g.grade_name,s.subject_name,COUNT(DISTINCT q.question_id) "question_count"
        FROM question_info q
        ,tp_j_course_question_info tr
        ,tp_course_info tc
        ,tp_j_course_teaching_material ttm
        ,teach_version_info tv
        ,teaching_material_info tm
        ,grade_info g
        ,subject_info s
        WHERE tr.`course_id` = ttm.`course_id`
        AND q.question_id = tr.`question_id`
        AND tc.`COURSE_ID` > 0
        AND tc.`COURSE_ID` = tr.`course_id`
        AND ttm.`teaching_material_id` = tm.`material_id`
        AND tm.`version_id` = tv.`version_id`
        AND tm.`grade_id` = g.`GRADE_ID`
        AND tm.`subject_id` = s.`SUBJECT_ID`
        AND tc.`LOCAL_STATUS` = 1
        AND q.`question_id` > 0 
        GROUP BY g.grade_id,s.subject_id
        ORDER BY g.grade_id,s.subject_id
             ''')
ques_grade_subject_count = mysqlDB(ques_grade_subject_sql)


#合并表格
#word_grade_subject_count - 课件
#word_mp4_grade_subject_count - 微课关联的学案
#mp4_grade_subject_count - 微课
#mp4_word_grade_subject_count - 关联学案的微课
#cpaper_grade_subject_count - 成卷
#paper_grade_subject_count - AB卷
#ques_grade_subject_count - 试题

res_dfs = [word_grade_subject_count,
           word_mp4_grade_subject_count,
           mp4_grade_subject_count,
           mp4_word_grade_subject_count,
           cpaper_grade_subject_count,
           paper_grade_subject_count,
           ques_grade_subject_count]
df_merge= reduce(lambda left,right: pd.merge(left,right,on=['grade_name','subject_name'],how = 'left'), res_dfs)


# 填充空缺数据为0
res_count = df_merge.fillna(0)


# resource_info
res_count.to_sql(name='resource_info', con=con,if_exists='replace', index=False)
print('同步成功')






def echarts_lin(x_data_list, y_name, y_data_list):
    # 渲染图大小
    line = Line(init_opts=opts.InitOpts(width='1000px'))

    # X轴数据
    line.add_xaxis(x_data_list)

    # Y轴名称和
    for i_name, i_reslut in zip(y_name, y_data_list):
        if i_name  not in ('学资源', '讨论', '单题',  '微课程', '一般任务', '直播课', '个性化', '先声'):
            line.add_yaxis('{}'.format(i_name), i_reslut, is_selected=True)
        else:
            line.add_yaxis('{}'.format(i_name), i_reslut, is_selected=False)

    # 全局配置
    line.set_global_opts(
        # 主标题
        title_opts=opts.TitleOpts(title=""),
        # 缩放功能
        datazoom_opts=opts.DataZoomOpts(is_show=True, type_='slider', range_start=50, range_end=100),
        tooltip_opts=opts.TooltipOpts(trigger="axis"),
        xaxis_opts=opts.AxisOpts(
            axislabel_opts={"interval": "0", "rotate": 45},
        ),

    )

    # 系统配置
    line.set_series_opts(
        # 图中显示数值
        label_opts=opts.LabelOpts(is_show=True),

        # 线装图，平均线
        markline_opts=opts.MarkLineOpts(
            data=[opts.MarkLineItem(type_="average", name="平均值"), ]
        ),
        # 点装图，最大值最小值
        markpoint_opts=opts.MarkPointOpts(
            data=[
                opts.MarkPointItem(type_="min", name="最小值"),
                opts.MarkPointItem(type_="max", name="最大值"),
            ]
        ),
    )

    return line



def echarts_bar(x_data, y_data):
    bar = Bar(init_opts=opts.InitOpts(width='1200px'))
    bar.add_xaxis(x_data)
    bar.add_yaxis("开通学校数量", y_data)
    bar.set_global_opts(
        title_opts=opts.TitleOpts(title=""),
        datazoom_opts=opts.DataZoomOpts(is_show=True, type_='slider',range_start = 70,range_end = 100),


    )

    bar.set_series_opts(
        label_opts=opts.LabelOpts(is_show=True),
        markline_opts=opts.MarkLineOpts(
            data=[

                opts.MarkLineItem(type_="average", name="平均值"),
            ]
        ),
        markpoint_opts=opts.MarkPointOpts(
            data=[
                opts.MarkPointItem(type_="min", name="最小值"),
                opts.MarkPointItem(type_="max", name="最大值"),
            ]
        ),
    )
    return bar




def echarts_pie(x_data_list,title_name):
    pie = Pie()
    pie.add(
            "",
            x_data_list,
            radius=["40get_ipython().run_line_magic("",", " \"75%\"],")
        )
    pie.set_global_opts(
            title_opts=opts.TitleOpts(title=title_name),
            legend_opts=opts.LegendOpts(orient="vertical", pos_top="15get_ipython().run_line_magic("",", " pos_left=\"2%\"),")
        )
    pie.set_series_opts(label_opts=opts.LabelOpts(formatter="{b}：{c} ({d}get_ipython().run_line_magic(")"))", "")


    return pie


school_sql = '''
        
SELECT fr.school_id ,fr.name ,s.province,s.city,fr.c_time,fr.validity_time
        from franchised_school_info fr,school_info s where fr.school_type in (3,4) 
        and fr.school_id  >50000 and fr.enable = 0  and s.school_id = fr.school_id 
        '''
school = mysqlDB(school_sql)


school.head()


school.sort_values(by = 'c_time', ascending = False).reset_index( drop = True).head()


school['c_dttime'] = school['c_time'].dt.strftime('get_ipython().run_line_magic("Y-%m')", "")
# school['c_dttime'] = pd.to_datetime(school['c_time'], format='get_ipython().run_line_magic("Y-%m').dt.date", "")

school.append(school['c_dttime'])
school_data = school['c_dttime'].groupby(school['c_dttime']).agg('count')
school_data['2017-09'] = 43



x_data = school_data.index.tolist()
y_data =  school_data.values.tolist()



bar1 = echarts_bar(x_data,y_data)
bar1.load_javascript()


bar1.render_notebook()


school_class_sql = '''
         SELECT fr.school_id,fr.name,count(distinct c.class_id) as '班级数', count(distinct axp.class_id) as '爱学派班级数', tea.tea_count as '教师人数',stu.stu_count as '学生人数'
        from  franchised_school_info fr LEFT JOIN class_info c on fr.school_id = c.dc_school_id
        LEFT JOIN ( SELECT c.DC_SCHOOL_ID , count(DISTINCT jc.user_id ) as 'tea_count' from j_class_user jc ,class_info c  where c.year = '2020~2021' and c.class_id = jc.class_id and jc.RELATION_TYPE = '任课老师' GROUP BY c.DC_SCHOOL_ID  ) tea  on tea.DC_SCHOOL_ID = fr.school_id
        LEFT JOIN ( SELECT c.DC_SCHOOL_ID , count(DISTINCT jc.user_id ) as 'stu_count' from j_class_user jc ,class_info c  where c.year = '2020~2021' and c.class_id = jc.class_id and jc.RELATION_TYPE = '学生' GROUP BY c.DC_SCHOOL_ID  ) stu on stu.DC_SCHOOL_ID = fr.school_id
        LEFT JOIN (SELECT class_id,DC_SCHOOL_ID from class_info where year = '2020~2021'  and axp_end_time >NOW() ) axp on axp.DC_SCHOOL_ID = fr.school_id
        where fr.school_type in (3,4) and fr.school_id  >50000 and fr.enable = 0 and c.year = '2020~2021' 
        group by fr.school_id
        '''
school_class = mysqlDB(school_class_sql)


school_class = school_class.fillna(0)
# 指定字段 数据类型。
# school[['school_id','tea_count','stu_count']] = school[['school_id','tea_count','stu_count']].astype({'school_id':str,'tea_count':'int64','stu_count':'int64'})
school_class.sort_values(by='爱学派班级数',ascending = False).head()


school_course_task_sql = 'SELECT * FROM school_course_task where classroom_id is null '
school_course_task = sqliteDB(school_course_task_sql)



# c_time转 datatime 格式
school_course_task['c_time'] = pd.to_datetime(school_course_task['c_time'] )



task_data = []
task_id = [1,2,3,4,6,7,10,13,14,15]
task_name = ['学资源','讨论','单题','测验','微课程','一般任务','直播课','答题卡','个性化','先声',]

for i in task_id:
    if i==7 :
        task_data.append((school_course_task.loc[((school_course_task['task_type']== 7))|(school_course_task['task_type']== 8)|(school_course_task['task_type']== 9)].drop_duplicates(['task_id'])).agg('count')['task_id'])
    else:
        task_data.append((school_course_task.loc[(school_course_task['task_type']== i)].drop_duplicates(['task_id'])).agg('count')['task_id'])

task_data_list = []
for i in range(0,10):
    task_data_list.append([task_name[i],int(task_data[i])])
    
print(task_data_list)




pie1 =echarts_pie(x_data_list = task_data_list,title_name = '任务分布比例')
pie1.load_javascript()



pie1.render_notebook()



school_course_task_data = (school_course_task.drop_duplicates(['task_id'])['c_time'].groupby(school_course_task['c_time'].dt.date)).agg('count')

school_course_task_x = school_course_task_data.index.tolist()
school_course_task_y = school_course_task_data.values.tolist()



reslut_name = ["任务数量"]
reslut_data = [school_course_task_y]

line1 = echarts_lin(x_data_list=school_course_task_x,y_name=reslut_name,y_data_list=reslut_data)
line1.load_javascript()


line1.render_notebook()


df_list = []

df_list.append(sqliteDB('SELECT date(c_time) day  FROM school_course_task  where classroom_id is null GROUP BY  day' ))

for i in task_id:
    
    if i == 7 :
        df_list.append( sqliteDB('SELECT date(c_time) day ,count(distinct task_id) as "7" FROM school_course_task   where task_type in (7,8,9) and classroom_id is null  GROUP BY  day' ))
                       
    else:
                       
        df_list.append( sqliteDB('SELECT date(c_time) day ,count(distinct task_id) as "{}"  FROM school_course_task   where task_type = {}  and  classroom_id is null GROUP BY  day'.format(i,i)) )


df_merge= reduce(lambda left,right: pd.merge(left,right,on=['day'],how = 'left'), df_list)


# 填充空缺数据为0
task_day_count = df_merge.fillna(0)

task_day_count.head()





task_day_count['3'] = task_day_count['3'].astype('int64')
task_day_count['10'] = task_day_count['10'].astype('int64')
task_day_count['14'] = task_day_count['14'].astype('int64')
task_day_count['15'] = task_day_count['15'].astype('int64')



every_day = task_day_count['day'].to_list()
task_every_day_count = []

for i in task_id:
    task_every_day_count.append(task_day_count['{}'.format(i)].to_list())

# print(task_every_day_count)



line1 = echarts_lin(x_data_list=every_day,y_name=task_name,y_data_list=task_every_day_count)

line1.load_javascript()


line1.render_notebook()


task13_taacher_sql = '''
        SELECT sc.school_id,sc.name ,sc.teacher_name,sc.grade_name ,sc.subject_name ,COUNT(DISTINCT task_id) as task_count ,hp.hp_count 
        from school_course_task   sc LEFT JOIN 
        (SELECT sc.school_id,sc.name ,sc.teacher_name,sc.grade_name ,sc.subject_name ,COUNT(DISTINCT task_id) as hp_count from school_course_task sc WHERE sc.task_type = 13 and sc.correct_model = 1
         GROUP BY sc.school_id,sc.c_user_id 
        )hp  on hp.school_id = sc.school_id and sc.teacher_name = hp.teacher_name  
        WHERE sc.task_type = 13   and sc.classroom_id is null
        GROUP BY sc.school_id,sc.c_user_id 
        ORDER BY COUNT(DISTINCT sc.task_id) DESC

'''
task13_taacher_count = sqliteDB(task13_taacher_sql)


task13_grade_sql = '''
        SELECT grade_name,count(DISTINCT task_id) from school_course_task where task_type = 13 and  classroom_id is null
        GROUP BY grade_name 
        ORDER BY count(DISTINCT task_id) DESC

'''
task13_grade_count = sqliteDB(task13_grade_sql)


task13_subject_sql = '''
        SELECT subject_name,count(DISTINCT task_id) from school_course_task where task_type = 13 and  classroom_id is null
        GROUP BY subject_name 
        ORDER BY count(DISTINCT task_id) DESC

'''
task13_subject_count = sqliteDB(task13_subject_sql)



task13_taacher_count['school_id'] = task13_taacher_count['school_id'].astype('str')
task13_taacher_count = task13_taacher_count.fillna(0)
task13_taacher_count.describe()


task13_taacher_count.sort_values(axis = 0 ,by = 'task_count',ascending = False).reset_index(drop = True)
task13_taacher_count.head()


# hp_data = hptask_13_count['c_time'].groupby(hptask_13_count['c_time'].dt.date).agg('count')

task13_taacher_count.groupby(task13_taacher_count['grade_name']).agg('sum')['task_count']


task13_grade_count


task13_grade_count.to_dict('records')


task13_subject_count


task13_sql = '''
       
        SELECT a.school_id,a.name,b.task13_count,c.hp_task13_count
        FROM "school_course_task" a  
        LEFT JOIN  (SELECT school_id ,count(DISTINCT task_id) as task13_count from school_course_task where task_type = 13 and classroom_id is null GROUP BY school_id) b  on a.school_id = b.school_id
        LEFT JOIN (SELECT school_id ,count(DISTINCT task_id) as hp_task13_count from school_course_task where  correct_model get_ipython().getoutput("= 0  and classroom_id is null  GROUP BY school_id) c on a.school_id = c.school_id")
        where a.classroom_id is null  
        GROUP BY a.school_id
        '''
task_13_count = sqliteDB(task13_sql)



task_13_count = task_13_count.fillna(0)
task_13_count = task_13_count.sort_values(axis = 0 ,by = 'task13_count',ascending = False).reset_index(drop = True)
task_13_count.head(20)


task_13_count = task_13_count.sort_values(axis = 0 ,by = 'hp_task13_count',ascending = False).reset_index(drop = True)
task_13_count.head(20)


task13_data = sqliteDB('SELECT date(c_time) day ,count(distinct task_id) as "13" FROM school_course_task   where task_type =13  GROUP BY  day' )
hp_data = sqliteDB('SELECT date(c_time) day ,count(distinct task_id) "hp"   FROM "school_course_task"   where task_type =13 and correct_model get_ipython().getoutput("=0  GROUP BY  day' )")
df_merge = pd.merge(task13_data,hp_data,on=['day'],how = 'left')
df_merge.head()



x_data = df_merge['day'].to_list()
y_data = [df_merge['13'].to_list(),df_merge['hp'].to_list()]
y_name_list = ['答题卡','答题卡互批']


line1 = echarts_lin(x_data_list=x_data,y_name=y_name_list,y_data_list=y_data)


line1.load_javascript()



line1.render_notebook()


axp_class_sql = ('''
    SELECT fr.school_id,fr.name , count(c.class_id)  as '爱学派班级数'
    from  class_info c, franchised_school_info fr  
    where c.axp_end_time >NOW()  and fr.school_type in (3,4) and fr.school_id =c.DC_SCHOOL_ID  and c.year = '2021~2022'
    group by  fr.name
    ORDER by '爱学派班级数' DESC
         ''')
axp_class = mysqlDB(axp_class_sql)



axp_class.sort_values(ascending = False ,by = '爱学派班级数' , axis = 0 ).head()


print('有效爱学派班级总数：',axp_class['爱学派班级数'].sum())


english_sql = ('''
     SELECT fr.school_id,fr.name ,count(DISTINCT a.user_id) as '先声学生有效人数',t.tea_count as '教师开通人数'
     FROM sing_sound_user_auth_config_info a,franchised_school_info fr ,(select school_id ,count(DISTINCT(user_id) ) as tea_count from teaching_j_user_module where object_type_id = 2 
     GROUP BY school_id ) t
     WHERE  a.end_time > a.c_time  AND a.school_id=fr.school_id and a.end_time >NOW()  and t.school_id = fr.school_id and fr.school_type in (3,4) and fr.school_id >50000 and fr.enable = 0
     GROUP BY a.school_id
         ''')
english_count = mysqlDB(english_sql)



english_count.head()


print('开通口语先声学生人数数：',english_count['先声学生有效人数'].sum())
print('开通口语先声总教师人数：',english_count['教师开通人数'].sum())


# school_count.columns.to_list()

school_describe = (course_file.iloc[:,5:]).describe()

school_dic = {
    '班级数':course_file['班级数'].sum( axis = 0),
     '教师人数':course_file['教师人数'].sum(),
     '学生人数':course_file['学生人数'].sum(),
     '爱学派班级数':course_file['爱学派班级数'].sum(),
     '课程数':course_file['课程数'].sum(),
     '学校任务总数':course_file['学校任务总数'].sum(),
     '学资源':course_file['学资源'].sum(),
     '讨论':course_file['讨论'].sum(),
     '单个试题':course_file['单个试题'].sum(),
     '测验':course_file['测验'].sum(),
     '微课':course_file['微课'].sum(),
     '一般任务（语音）':course_file['一般任务（语音）'].sum(),
     '一般任务（文字）':course_file['一般任务（文字）'].sum(),
     '一般任务（图片）':course_file['一般任务（图片）'].sum(),
     '直播课':course_file['直播课'].sum(),
     '答题卡':course_file['答题卡'].sum(),
     '个性化':course_file['个性化'].sum(),
     '先声':course_file['先声'].sum(),
     '先声学生有效人数':course_file['先声学生有效人数'].sum(axis = 0),
     '教师开通人数':course_file['教师开通人数'].sum(),
     '答题卡数量':course_file['答题卡数量'].sum(),
     '教师批改':course_file['教师批改'].sum(),
     '学生互批':course_file['学生互批'].sum()
}
school_df = pd.DataFrame(school_dic,index = ['sum'])
school_describe = school_describe.append(school_df)
school_describe


res_count


res_count.to_excel('../jupyter/resource/平台资源数据get_ipython().run_line_magic("s.xls'%now_time)", "")
print('导出成功')




tea_name_sql = ('''
    SELECT s.name,s.school_id ,u.ETT_USER_ID,oc.user_name , oc.password,t.TEACHER_NAME ,u.state_id, c.CLASS_ID,c.CLASS_GRADE,c.CLASS_NAME,c.PATTERN,sb.SUBJECT_NAME,jc.C_TIME
    from  oracle2utf.coschuser_info oc,user_info u,school_info s,teacher_info t ,j_class_user jc ,class_info c,subject_info sb
    where  oc.jid = u.ETT_USER_ID and u.DC_SCHOOL_ID = s.school_id and u.ref = t.user_id  and u.ref = jc.user_id  and jc.CLASS_ID = c.CLASS_ID and c.`YEAR` = '2020~2021' and jc.SUBJECT_ID = sb.SUBJECT_ID
    and oc.user_name ='大连测试001' 
    GROUP BY s.school_id ,u.ETT_USER_ID,c.CLASS_GRADE,c.CLASS_NAME,sb.SUBJECT_NAME
         ''')
tea_name = mysqlDB(tea_name_sql)
tea_name


tea_name_json =  tea_name.to_dict('records')



tea_name_json


tea_name_json = json.loads(tea_name_json)
tea_name_json


for key in tea_name_json.keys():
        print(tea_name_json['{}'.format(key)]['{}'.format(i)])





tea_name_sql = ('''
    SELECT s.name,s.school_id ,u.ETT_USER_ID,oc.user_name , oc.password,t.TEACHER_NAME ,u.state_id, c.CLASS_ID,c.CLASS_GRADE,c.CLASS_NAME,c.PATTERN,sb.SUBJECT_NAME,jc.C_TIME
    from  oracle2utf.coschuser_info oc,user_info u,school_info s,teacher_info t ,j_class_user jc ,class_info c,subject_info sb
    where  oc.jid = u.ETT_USER_ID and u.DC_SCHOOL_ID = s.school_id and u.ref = t.user_id  and u.ref = jc.user_id  and jc.CLASS_ID = c.CLASS_ID and c.`YEAR` = '2020~2021' and jc.SUBJECT_ID = sb.SUBJECT_ID
    and oc.jid =  1007798979
    GROUP BY s.school_id ,u.ETT_USER_ID,c.CLASS_GRADE,c.CLASS_NAME,sb.SUBJECT_NAME
         ''')
tea_name = mysqlDB(tea_name_sql)
tea_name


stu_name_sql = ('''
    SELECT s.name,s.school_id ,u.ETT_USER_ID,oc.user_name , oc.password,t.STU_NAME ,u.state_id, c.CLASS_ID,c.CLASS_GRADE,c.CLASS_NAME,jc.C_TIME
    from  oracle2utf.user_info oc,user_info u,school_info s,student_info t ,j_class_user jc ,class_info c
    where  oc.user_id = u.ETT_USER_ID and u.DC_SCHOOL_ID = s.school_id and u.ref = t.user_id  and u.ref = jc.user_id  and jc.CLASS_ID = c.CLASS_ID and c.`YEAR` = '2020~2021'
    and oc.user_name ='大连测试002'
         ''')
stu_name = mysqlDB(stu_name_sql)
stu_name


stu1 =stu_name.to_json(orient = 'index', force_ascii = False)
import json
stu1 = json.loads(stu1)
type(stu1)


stu_name_sql = ('''
    SELECT s.name,s.school_id ,u.ETT_USER_ID,oc.user_name , oc.password,t.STU_NAME ,u.state_id, c.CLASS_ID,c.CLASS_GRADE,c.CLASS_NAME,jc.C_TIME
    from  oracle2utf.user_info oc,user_info u,school_info s,student_info t ,j_class_user jc ,class_info c
    where  oc.user_id = u.ETT_USER_ID and u.DC_SCHOOL_ID = s.school_id and u.ref = t.user_id  and u.ref = jc.user_id  and jc.CLASS_ID = c.CLASS_ID and c.`YEAR` = '2020~2021'
    and oc.user_id = 1007686434
         ''')
stu_name = mysqlDB(stu_name_sql)
stu_name


body = '''{
  "query": {
    "bool": {
      "must": [
        {
          "term": {
            "jid": {
              "value": "1005223605"
            }
          }
        }
      ]
    }
  },"sort": [
    {
      "c_time": {
        "order": "asc"
      }
    }
  ],"size":10000
  
}'''
res = es.search(index="two_month_action_logs", body=body)
res 
res = json.dumps(res)
# res = str(res)

with open('C:/Users/caozhiqiang/Desktop/行为数据.txt','a') as file:
    file.write(res)
    file.write('\n')


el = pd.ExcelFile('F:/2、项目版本迭代/1、教学平台/102、考试分析/测试数据/学生每题作答 - 数据.xlsx')
sheet_names = el.sheet_names

for sheet_name in sheet_names:
    excel_file = pd.read_excel('F:/2、项目版本迭代/1、教学平台/102、考试分析/测试数据/学生每题作答 - 数据.xlsx',sheet_name='{}'.format(sheet_name))
    excel_file.to_csv('F:/2、项目版本迭代/1、教学平台/102、考试分析/测试数据/csv/{}.csv'.format(sheet_name))

    
    
