import csv
import decimal
import psycopg2

username = 'student01'
password = '9959'
database = 'student01_DB'

INPUT_CSV_FILE = 'StudentsPerformance.csv'

query_0 = '''
CREATE TABLE student_stats_imported
(
    student_id char(10) NOT NULL,
    level char(10) NOT NULL,
    test_id char(10) NOT NULL,
    score char(10) NOT NULL,
    CONSTRAINT pk_social_network_ads PRIMARY KEY (student_id, level, test_id)
)
'''

query_1 = '''
DELETE FROM student_stats_imported
'''

query_2 = '''
INSERT INTO student_stats_imported (student_id, level, test_id, score) VALUES (%s, %s, %s, %s)
'''

def level_definition(strng):
    if strng == 'bachelor\'s degree':
        return 'BS'
    elif strng == 'master\'s degree':
        return 'MS'
    elif strng == 'associate\'s degree':
        return 'AS'
    elif strng == 'some college':
        return 'SC'
    elif strng == 'high school':
        return 'HS'
    


conn = psycopg2.connect(user=username, password=password, dbname=database)

with conn:
    cur = conn.cursor()
    #cur.execute(query_1)
    cur.execute(query_0)    
    with open(INPUT_CSV_FILE, 'r') as inf:
        reader = csv.DictReader(inf)
        for idx, row in enumerate(reader):
            level = level_definition(row['parental level of education'])
            values = (idx + 1, level, 'MTH', row['math score']) 
            cur.execute(query_2, values)
            values = (idx + 1, level, 'RDG', row['reading score']) 
            cur.execute(query_2, values)
            values = (idx + 1, level, 'WRT', row['writing score']) 
            cur.execute(query_2, values)

    conn.commit()