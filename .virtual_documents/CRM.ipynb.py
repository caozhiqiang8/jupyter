import pandas as pd
from sqlalchemy import create_engine
from functools import reduce
import matplotlib as mpl
import matplotlib.pyplot as plt 

from pyecharts import options as opts
from pyecharts.charts import Line,Bar,Pie

# from pyecharts.globals import ThemeType
from pyecharts.globals import CurrentConfig, NotebookType
CurrentConfig.NOTEBOOK_TYPE = NotebookType.JUPYTER_LAB



# datafram显示行数和列数
# pd.options.display.max_columns = None
# pd.options.display.max_rows = None

pd.set_option('max_columns', 20)
pd.set_option('max_rows', 10)



get_ipython().run_cell_magic("HTML", "", """<style type="text/css">
table.dataframe td, table.dataframe th {
    border: 1px  black solid !important;
  color: black !important;
}""")


#链接mysql
def mysqlDB(sql):
    engine = create_engine(
        'mysql+pymysql://schu:slavep@123.103.75.152:3306/school')
    result = pd.read_sql_query(sql = sql, con = engine)
    return result


#柱状图
def echarts_bar(x_data, y_data):
    bar = Bar(init_opts=opts.InitOpts(width='1200px'))
    bar.add_xaxis(x_data)
    bar.add_yaxis("开通学校数量", y_data)
    bar.set_global_opts(
        title_opts=opts.TitleOpts(title=""),
        datazoom_opts=opts.DataZoomOpts(is_show=True, type_='slider',range_start = 70,range_end = 100,pos_top ='95get_ipython().run_line_magic("'),", "")
        xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=15)),


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


#饼图
def echarts_pie(x_data_list,title_name):
    pie = Pie(init_opts=opts.InitOpts(height='500px',width='1200px'))
    pie.add(
            "",
            x_data_list,
            radius=["40get_ipython().run_line_magic("",", " \"75%\"],")
        )
    pie.set_global_opts(
            title_opts=opts.TitleOpts(title=title_name),
            legend_opts=opts.LegendOpts(orient="vertical", pos_top="10get_ipython().run_line_magic("",", " pos_left=\"5%\"),")
            
        
        )
    pie.set_series_opts(label_opts=opts.LabelOpts(formatter="{b}：{c} ({d}get_ipython().run_line_magic(")"))", "")


    return pie


sql = '''
SELECT fr.school_id,fr.name ,count(*) as task_count from franchised_school_info fr , tp_task_info  tt, tp_course_info tc where tt.course_id = tc.course_id  and tc.DC_SCHOOL_ID = fr.school_id  and tt.c_time >'2021-7-15'
and fr.school_type in (3,4) and tt.CLASSROOM_ID is null and fr.ENABLE = 0
GROUP BY fr.school_id 
ORDER BY  task_count desc
'''
school_data = mysqlDB(sql)
school_data


task_id = [1,2,3,4,6,7,10,13,14,15]
task_name = ['学资源','讨论','单题','测验','微课程','一般任务','直播课','答题卡','个性化','先声',]

df = [school_data,]
for i in task_id:
    if i ==7:
        sql = '''
        SELECT fr.school_id,fr.name ,count(*) as task_count from franchised_school_info fr , tp_task_info  tt, tp_course_info tc where tt.course_id = tc.course_id  and tc.DC_SCHOOL_ID = fr.school_id  and tt.c_time >'2021-7-15'
        and fr.school_type in (3,4) and tt.CLASSROOM_ID is null and fr.ENABLE = 0 and tt.task_type in (7,8,9)
        GROUP BY fr.school_id 
        '''
        df.append(mysqlDB(sql))
        
    else:
        sql = '''
        SELECT fr.school_id,fr.name ,count(*) as task_count from franchised_school_info fr , tp_task_info  tt, tp_course_info tc where tt.course_id = tc.course_id  and tc.DC_SCHOOL_ID = fr.school_id  and tt.c_time >'2021-7-15'
        and fr.school_type in (3,4) and tt.CLASSROOM_ID is null and fr.ENABLE = 0 and tt.task_type = {}
        GROUP BY fr.school_id 
        '''.format(i)
        df.append(mysqlDB(sql))
    
data = reduce(lambda left, right: pd.merge(left, right, on=['school_id','name'], how='left'), df)
data = data.fillna(0)
column = ['学校ID','学校名称','总数','学资源','讨论','单题','测验','微课程','一般任务','直播课','答题卡','个性化','先声',]
data.columns = column
data




data.to_excel(r'C:\Users\caozhiqiang\Desktop\本学期学校任务类型分布.xlsx')


year_list = []
for i in range(2014,2022):
    year_list.append('{}~{}上'.format(i,i+1))
    year_list.append('{}~{}下'.format(i,i+1))

year_list




school_year_list = [
'2014~2015上',
 '2014~2015下',
 '2015~2016上',
 '2015~2016下',
 '2016~2017上',
 '2016~2017下',
 '2017~2018上',
 '2017~2018下',
 '2018~2019上',
 '2018~2019下',
 '2019~2020上',
 '2019~2020下',
 '2020~2021上',
 '2020~2021下',
 '2021~2022上',]
year = [
    
    ['2014-7-15','2015-1-15'],
    ['2015-1-15','2015-7-15'],
    
    ['2015-7-15','2016-1-15'],
    ['2016-1-15','2016-7-15'],
    
    ['2016-7-15','2017-1-15'],
    ['2017-1-15','2017-7-15'],
    
    ['2017-7-15','2018-1-15'],
    ['2018-1-15','2018-7-15'],
    
    ['2018-7-15','2019-1-15'],
    ['2019-1-15','2019-7-15'],
    
    ['2019-7-15','2020-1-15'],
    ['2020-1-15','2020-7-15'],
    
    ['2020-7-15','2021-1-15'],
    ['2021-1-15','2021-7-15'],
    
    ['2021-7-15','2022-1-15']
]


sql = '''
SELECT school_id ,name,c_time from franchised_school_info fr where  fr.school_type in (3,4)  and  fr.school_id > 50000
ORDER BY c_time ASC
'''
school = mysqlDB(sql)


school_year_df =[school,]


for i in year:
    sql = '''
    SELECT fr.school_id,fr.name ,count(*) as task_count from franchised_school_info fr , tp_task_info  tt, tp_course_info tc where tt.course_id = tc.course_id  and tc.DC_SCHOOL_ID = fr.school_id  
    and tt.c_time >'{}' and tt.c_time <='{}'
and fr.school_type in (3,4) and tt.CLASSROOM_ID is null and fr.ENABLE = 0
GROUP BY fr.school_id 
    '''.format(i[0],i[1])
    school_year_df.append(mysqlDB(sql))
    
school_year_data = reduce(lambda left, right: pd.merge(left, right, on=['school_id','name'], how='left'), school_year_df) 

school_year_data.columns = ['学校ID','学校名称','创建时间','2014~2015上',
 '2014~2015下',
 '2015~2016上',
 '2015~2016下',
 '2016~2017上',
 '2016~2017下',
 '2017~2018上',
 '2017~2018下',
 '2018~2019上',
 '2018~2019下',
 '2019~2020上',
 '2019~2020下',
 '2020~2021上',
 '2020~2021下',
 '2021~2022上',]



school_year_data


school_year_data
school_year = school_year_data.fillna(0)


task = 30


school_year['zc15'] = (school_year[school_year_list[0]] >= task) &  (school_year[school_year_list[1]] >= task) & (school_year[school_year_list[2]] >= task) & (school_year[school_year_list[3]] >= task) &(school_year[school_year_list[4]] >= task) & (school_year[school_year_list[5]] >= task) & (school_year[school_year_list[6]] >= task) & (school_year[school_year_list[7]] >= task) &(school_year[school_year_list[8]] >= task) & (school_year[school_year_list[9]] >= task) & (school_year[school_year_list[10]] >= task) & (school_year[school_year_list[11]] >= task) & (school_year[school_year_list[12]] >= task) & (school_year[school_year_list[13]] >= task) & (school_year[school_year_list[14]] >= task)
school_year['zc14'] = (school_year[school_year_list[1]] >= task) & (school_year[school_year_list[2]] >= task) & (school_year[school_year_list[3]] >= task) &(school_year[school_year_list[4]] >= task) & (school_year[school_year_list[5]] >= task) & (school_year[school_year_list[6]] >= task) & (school_year[school_year_list[7]] >= task) &(school_year[school_year_list[8]] >= task) & (school_year[school_year_list[9]] >= task) & (school_year[school_year_list[10]] >= task) & (school_year[school_year_list[11]] >= task) & (school_year[school_year_list[12]] >= task) & (school_year[school_year_list[13]] >= task) & (school_year[school_year_list[14]] >= task)
school_year['zc13'] = (school_year[school_year_list[2]] >= task) & (school_year[school_year_list[3]] >= task) &(school_year[school_year_list[4]] >= task) & (school_year[school_year_list[5]] >= task) & (school_year[school_year_list[6]] >= task) & (school_year[school_year_list[7]] >= task) &(school_year[school_year_list[8]] >= task) & (school_year[school_year_list[9]] >= task) & (school_year[school_year_list[10]] >= task) & (school_year[school_year_list[11]] >= task) & (school_year[school_year_list[12]] >= task) & (school_year[school_year_list[13]] >= task) & (school_year[school_year_list[14]] >= task)
school_year['zc12'] = (school_year[school_year_list[3]] >= task) &(school_year[school_year_list[4]] >= task) & (school_year[school_year_list[5]] >= task) & (school_year[school_year_list[6]] >= task) & (school_year[school_year_list[7]] >= task) &(school_year[school_year_list[8]] >= task) & (school_year[school_year_list[9]] >= task) & (school_year[school_year_list[10]] >= task) & (school_year[school_year_list[11]] >= task) & (school_year[school_year_list[12]] >= task) & (school_year[school_year_list[13]] >= task) & (school_year[school_year_list[14]] >= task)
school_year['zc11'] = (school_year[school_year_list[4]] >= task) & (school_year[school_year_list[5]] >= task) & (school_year[school_year_list[6]] >= task) & (school_year[school_year_list[7]] >= task) &(school_year[school_year_list[8]] >= task) & (school_year[school_year_list[9]] >= task) & (school_year[school_year_list[10]] >= task) & (school_year[school_year_list[11]] >= task) & (school_year[school_year_list[12]] >= task) & (school_year[school_year_list[13]] >= task) & (school_year[school_year_list[14]] >= task)
school_year['zc10'] = (school_year[school_year_list[5]] >= task) & (school_year[school_year_list[6]] >= task) & (school_year[school_year_list[7]] >= task) &(school_year[school_year_list[8]] >= task) & (school_year[school_year_list[9]] >= task) & (school_year[school_year_list[10]] >= task) & (school_year[school_year_list[11]] >= task) & (school_year[school_year_list[12]] >= task) & (school_year[school_year_list[13]] >= task) & (school_year[school_year_list[14]] >= task)
school_year['zc9'] = (school_year[school_year_list[6]] >= task) & (school_year[school_year_list[7]] >= task) &(school_year[school_year_list[8]] >= task) & (school_year[school_year_list[9]] >= task) & (school_year[school_year_list[10]] >= task) & (school_year[school_year_list[11]] >= task) & (school_year[school_year_list[12]] >= task) & (school_year[school_year_list[13]] >= task) & (school_year[school_year_list[14]] >= task)
school_year['zc8'] = (school_year[school_year_list[7]] >= task) &(school_year[school_year_list[8]] >= task) & (school_year[school_year_list[9]] >= task) & (school_year[school_year_list[10]] >= task) & (school_year[school_year_list[11]] >= task) & (school_year[school_year_list[12]] >= task) & (school_year[school_year_list[13]] >= task) & (school_year[school_year_list[14]] >= task)
school_year['zc7'] = (school_year[school_year_list[8]] >= task) & (school_year[school_year_list[9]] >= task) & (school_year[school_year_list[10]] >= task) & (school_year[school_year_list[11]] >= task) & (school_year[school_year_list[12]] >= task) & (school_year[school_year_list[13]] >= task) & (school_year[school_year_list[14]] >= task)
school_year['zc6'] = (school_year[school_year_list[9]] >= task) & (school_year[school_year_list[10]] >= task) & (school_year[school_year_list[11]] >= task) & (school_year[school_year_list[12]] >= task) & (school_year[school_year_list[13]] >= task) & (school_year[school_year_list[14]] >= task)
school_year['zc5'] = (school_year[school_year_list[12]] >= task) & (school_year[school_year_list[11]] >= task) & (school_year[school_year_list[12]] >= task) & (school_year[school_year_list[13]] >= task) & (school_year[school_year_list[14]] >= task)
school_year['zc4'] = (school_year[school_year_list[11]] >= task) & (school_year[school_year_list[12]] >= task) & (school_year[school_year_list[13]] >= task) & (school_year[school_year_list[14]] >= task)
school_year['zc3'] = (school_year[school_year_list[12]] >= task) & (school_year[school_year_list[13]] >= task) & (school_year[school_year_list[14]] >= task)
school_year['zc2'] = (school_year[school_year_list[13]] >= task) & (school_year[school_year_list[14]] >= task)

school_year['ls'] = ((school_year[school_year_list[0]] >= task) | (school_year[school_year_list[1]] >= task) | (school_year[school_year_list[2]] >= task) | (school_year[school_year_list[3]] >= task) | (school_year[school_year_list[4]] >= task) | (school_year[school_year_list[5]] >= task) | (school_year[school_year_list[6]] >= task) | (school_year[school_year_list[7]] >= task) | (school_year[school_year_list[8]] >= task) | (school_year[school_year_list[9]] >= task) | (school_year[school_year_list[10]] >= task) | (school_year[school_year_list[11]] >= task) | (school_year[school_year_list[12]] >= task) | (school_year[school_year_list[13]] >= task)) & (school_year[school_year_list[14]] < task) 

school_year['new'] = (school_year[school_year_list[0]] == 0 ) & (school_year[school_year_list[1]] == 0 ) & (school_year[school_year_list[2]] == 0 ) & (school_year[school_year_list[3]] == 0 ) & (school_year[school_year_list[4]] == 0 ) & (school_year[school_year_list[5]] == 0) & (school_year[school_year_list[6]] == 0 ) & (school_year[school_year_list[7]] == 0 ) & (school_year[school_year_list[8]] == 0) & (school_year[school_year_list[9]] == 0 ) & (school_year[school_year_list[10]] == 0 ) & (school_year[school_year_list[11]] == 0 ) & (school_year[school_year_list[12]] == 0 ) & (school_year[school_year_list[13]] == 0 ) & (school_year[school_year_list[14]] >= task)  

school_year['js'] = (school_year[school_year_list[0]] < task) & (school_year[school_year_list[1]] < task) &  (school_year[school_year_list[2]] < task) & (school_year[school_year_list[3]] < task) & (school_year[school_year_list[4]] < task) & (school_year[school_year_list[5]] < task) & (school_year[school_year_list[6]] < task) & (school_year[school_year_list[7]] < task) & (school_year[school_year_list[8]] < task) & (school_year[school_year_list[9]] < task) & (school_year[school_year_list[10]] < task) & (school_year[school_year_list[11]] < task) & (school_year[school_year_list[12]] < task) & (school_year[school_year_list[13]] < task) & (school_year[school_year_list[14]] < task)

school_year = school_year.replace(to_replace=False,value='')
school_year


def schoolType(x):
    if x['new'] == True:
        return '新开学校'
    elif x['ls'] == True:
        return '流失学校'
    elif x['js'] == True:
        return '僵尸学校'
    elif x['zc15'] == True:
        return '忠诚15个学期'
    elif x['zc14'] == True:
        return '忠诚14个学期'
    elif x['zc13'] == True:
        return '忠诚13个学期'
    elif x['zc12'] == True:
        return '忠诚12个学期'
    elif x['zc11'] == True:
        return '忠诚11个学期'
    elif x['zc10'] == True:
        return '忠诚10个学期'
    elif x['zc9'] == True:
        return '忠诚9个学期'
    elif x['zc8'] == True:
        return '忠诚8个学期'
    elif x['zc7'] == True:
        return '忠诚7个学期'
    elif x['zc6'] == True:
        return '忠诚6个学期'
    elif x['zc5'] == True:
        return '忠诚5个学期'
    elif x['zc4'] == True:
        return '忠诚4个学期'
    elif x['zc3'] == True:
        return '忠诚3个学期'
    elif x['zc2'] == True:
        return '忠诚2个学期'
    else:
        return '回归学校'


school_year['type'] = school_year.apply(lambda x : schoolType(x),axis=1 )
school_year


school_year.to_excel(r'C:\Users\caozhiqiang\Desktop\学校群体划分1.xlsx')


school_sql = '''
        
SELECT fr.school_id ,fr.name ,s.province,s.city,fr.c_time,fr.validity_time
        from franchised_school_info fr,school_info s where fr.school_type in (3,4) 
        and fr.school_id  >50000 and fr.enable = 0  and s.school_id = fr.school_id 
        '''
school = mysqlDB(school_sql)


school['c_dttime'] = school['c_time'].dt.strftime('get_ipython().run_line_magic("Y-%m')", "")
# school['c_dttime'] = pd.to_datetime(school['c_time'], format='get_ipython().run_line_magic("Y-%m').dt.date", "")

school.append(school['c_dttime'])
school_data = school['c_dttime'].groupby(school['c_dttime']).agg('count')
school_data['2017-09'] = 43



x_data = school_data.index.tolist()
y_data =  school_data.values.tolist()
school_date_bar = echarts_bar(x_data,y_data)



school_date_bar.load_javascript()



school_date_bar.render_notebook()


school_count_df = []

for i in year:
    sql = '''
    SELECT count(fr.school_id ) as school_count from franchised_school_info fr where  fr.school_type in (3,4)  and fr.c_time >='{}' and fr.c_time <'{}'
    '''.format(i[0],i[1])
    school_count_df.append(mysqlDB(sql))
school_year_df =  pd.concat(school_count_df, axis=1,ignore_index=True)
school_year_df.columns = school_year_list
school_year_df



x_data = school_year_df.columns.tolist()
y_data = school_year_df.values.tolist()[0]


school_year_bar = echarts_bar(x_data,y_data)
school_year_bar.render_notebook()


school_count_df = []

for i in year:
    sql = '''
    SELECT count(fr.school_id ) as school_count from franchised_school_info fr where  fr.school_type in (3,4)  and fr.c_time <'{}'
    '''.format(i[1])
    school_count_df.append(mysqlDB(sql))
school_year_df =  pd.concat(school_count_df, axis=1,ignore_index=True)
school_year_df.columns = school_year_list
school_year_df



x_data = school_year_df.columns.tolist()
y_data = school_year_df.values.tolist()[0]


school_year_bar = echarts_bar(x_data,y_data)
school_year_bar.render_notebook()


data = school_year['type'].value_counts()
x = data.index.tolist()
y = data.values.tolist()
data_list = []

for i in range(0,17):
    data_list.append([x[i],int(y[i])])
data_list


pie1 = echarts_pie(data_list,'学校群体分布')



pie1.render_notebook()


lxc_sql = '''
SELECT fr.school_id,fr.name,os.name as belong_name ,g.grade_name ,s.subject_name,t.teacher_name ,count( DISTINCT tt.task_id ) as lxc_count
            from tp_task_info  tt, franchised_school_info fr ,as_answer_sheet_info aa ,oracle2utf.school_info os ,subject_info s,grade_info g,teacher_info t
            where    fr.school_id not in ( 50043,51613,50041,53741,50068,53535,50044,100002368)  and fr.SCHOOL_ID = aa.dc_school_id  and s.subject_id = aa.subject_id and g.grade_id = aa.grade_id 
            and tt.task_value_id = aa.paper_id  and aa.workbook_paper = 1 and os.school_id = fr.belong_school_id and aa.c_time >'2021-7-15'  and t.user_id = tt.c_user_id  and tt.CLASSROOM_ID is null 
            GROUP BY fr.school_id,aa.grade_id ,aa.SUBJECT_ID,tt.c_user_id
                        '''
lxc_school = mysqlDB(lxc_sql)
lxc_school_list = tuple(set(lxc_school['school_id'].tolist()))
print(lxc_school_list)



dtk_sql = '''
SELECT fr.school_id,fr.name,os.name as belong_name ,g.grade_name ,s.subject_name,t.teacher_name ,count( DISTINCT tt.task_id ) as dtk_count
            from tp_task_info  tt, franchised_school_info fr ,as_answer_sheet_info aa ,oracle2utf.school_info os ,subject_info s,grade_info g,teacher_info t
            where    fr.school_id  in {} and fr.SCHOOL_ID = aa.dc_school_id  and s.subject_id = aa.subject_id and g.grade_id = aa.grade_id 
            and tt.task_value_id = aa.paper_id  and aa.workbook_paper = 0 and os.school_id = fr.belong_school_id and aa.c_time >'2021-7-15'  and t.user_id = tt.c_user_id  and tt.CLASSROOM_ID is null 
            GROUP BY fr.school_id,aa.grade_id ,aa.SUBJECT_ID,tt.c_user_id
'''.format(lxc_school_list)

df =[mysqlDB(dtk_sql),lxc_school]

school_task_count = reduce(lambda left, right: pd.merge(left, right, on=['school_id','name','belong_name','grade_name','subject_name','teacher_name'], how='left'), df) 

school_task_count


school_task_count.sort_values(by=['school_id','lxc_count','grade_name','subject_name'],axis=0,ascending=False)


school_task_count.groupby('school_id').sum()



school_task_count.groupby('school_id')['teacher_name'].count()


school_task_count.to_excel(r'C:\Users\caozhiqiang\Desktop\学校年级学科任务分布.xlsx')
