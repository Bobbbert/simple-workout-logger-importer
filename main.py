import sqlite3
import pandas as pd
import shutil
import os

db_src_path = './'
db_src_fn = 'workoutlog.bak'
db_dst_path = '/mnt/c/Users/rfm/Downloads/'
db_dst_fn = 'workoutlog_experimental.bak'

print(os.getcwd())
workouts_fn = './fitx.csv'
categories_fn = './fitx_categories.csv'

# copy file for experimenting
shutil.copy(os.path.join(db_src_path, db_src_fn),
            os.path.join(db_dst_path, db_dst_fn))

con = sqlite3.connect(os.path.join(db_dst_path, db_dst_fn))
print("Connected to database.")

cur = con.cursor()

df_categories =pd.DataFrame(pd.read_csv(categories_fn))
df_workouts = pd.DataFrame(pd.read_csv(workouts_fn))
print("Loaded categories.")

# [id exercise type comment]
for i, row in df_categories.iterrows():
    existing_row = cur.execute("select * from exercises where exercise = '{}'".format(row['Übung']))

    if list(existing_row) == []:
        # insert if not existing
        cur.execute("insert into exercises values (NULL, ?, ?, ?)",
                    (row['Übung'], row['Typ'], None))
        con.commit()

print(df_workouts.columns)

# populate workouts
def insert_workout(workout):
    # workout
    date = workout.iloc[0]['Datum']
    time = "00:00"
    exercise_id = next(cur.execute("select id from exercises where exercise = '{}'".format(workout.iloc[0]['Gerät'])))
    exercise = workout.iloc[0]['Gerät']
    _type = next(cur.execute("select type from exercises where exercise = '{}'".format(workout.iloc[0]['Gerät'])))
    comment = None

    cur.execute("insert into workouts values (NULL, ?, ?, ?, ?, ?, ?)",
                (date, time, exercise_id[0], exercise, _type[0], comment))

    date_id = next(cur.execute("select id from workouts order by id desc limit 1"))[0]

    if workout.iloc[0]['Typ'] == 'cardio':
        for i, row in workout.iterrows():
            time = "{}:{}:{}".format("00", "0" + row['Wert'][0], row['Wert'][2:4])
            distance = row['Wert (Sub)']
            heart = None
            calories = None

            if row['Wert'] != 'None' and row['Wert (Sub)'] != 'None':
                cur.execute("insert into cardio values (NULL, ?, ?, ?, ?, ?)",
                            (date_id, time, distance, heart, calories))
            else:
                print("Invalid entry for cardio.")
    elif workout.iloc[0]['Typ'] == 'strength':
        for i, row in workout.iterrows():
            if row['Wert'] != 'None' and row['Wert (Sub)'] != 'None':
                cur.execute("insert into reps values (NULL, ?, ?, ?)",
                            (date_id, row['Wert'], row['Wert (Sub)']))
            else:
                print("Invalid enry for strength")
    else:
        raise Exception()

    con.commit()

df_workouts.groupby(['Datum', 'Gerät']).apply(insert_workout)
con.close()
