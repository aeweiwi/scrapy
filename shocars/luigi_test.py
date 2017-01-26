# !/usr/bin/env python
# -*- coding: utf-8 -*-
import luigi
from shocars import luigi_cars_crawler
import pandas as pd
import sqlalchemy
import sys
reload(sys)  # Reload does the trick!
sys.setdefaultencoding('UTF-8')
from sqlalchemy.types import to_instance, TypeEngine
import datetime
import matplotlib.pyplot as plt
import sqlite3


class Analyize(luigi.Task):

    def requires(self):
        return luigi_cars_crawler()

    def output(self):
        luigi.LocalTarget('stat.csv')

    def run(self):

        df = pd.read_csv(self.input().open('r'),
                         error_bad_lines=False,
                         encoding='utf-8')

        output_df = df.head(5)
        output_df.to_csv(self.output())
        output_df.to_csv('stat.csv')


class daily_analysis(luigi.Task):

    def requires(self):
        return to_mydb()

    def output(self):
        today = datetime.datetime.now()
        today = today.strftime('%Y-%m-%d')
        return luigi.LocalTarget('daily_stat_{0}.pdf'.format(today))

    def run(self):
        con = sqlite3.connect("my_db.sqlite")
        df = pd.read_sql_query(
            """SELECT * from shocars""", con)

        import ipdb
        ipdb.set_trace()
        daily_stat = df.groupby(df.columns[17])[df.columns[0]].count()
        stat_plot = daily_stat.plot(kind='bar', figsize=(6, 6),
                                    legend=False,
                                    use_index=False,
                                    subplots=True,
                                    colormap="Pastel1")
        # fig = stat_plot[0].get_figure()
        with self.output().open('w') as out_file:
            plt.savefig(out_file)


class to_mydb(luigi.Task):
    chunksize = 1

    def output(self):
        today = datetime.datetime.now()
        today = today.strftime('%Y-%m-%d')
        return luigi.LocalTarget('mydb_output_{0}.log'.format(
            today))

    def requires(self):
        return luigi_cars_crawler()

    def run(self):

        def to_sql_k(self, frame, name, if_exists='fail', index=True,
                     index_label=None, schema=None,
                     chunksize=None, dtype=None, **kwargs):

            table = pd.io.sql.SQLTable(name, self, frame=frame, index=index,
                                       if_exists=if_exists,
                                       index_label=index_label,
                                       schema=schema, dtype=dtype, **kwargs)
            table.create()
            table.insert(chunksize)

        engine = sqlalchemy.create_engine('sqlite:///my_db.sqlite')
        pandas_sql = pd.io.sql.pandasSQL_builder(engine,
                                                 schema=None,
                                                 flavor=None)

        df = pd.read_csv(self.input().open('r'),
                         error_bad_lines=False,
                         encoding='utf-8')

        for i in range(0, df.shape[0], self.chunksize):
            try:
                to_sql_k(pandas_sql,
                         df.iloc[i:i * self.chunksize + 1,
                                 slice(None)],
                         'shocars',
                         index=False,
                         keys=['hash'],
                         if_exists='append')

            except Exception as e:
                # print e
                pass
        with self.output().open('w') as out_file:
            out_file.write("Done")

if __name__ == '__main__':
    # luigi.run(["--local-scheduler"], main_task_cls=to_mydb)
    # luigi.run(["--local-scheduler"], main_task_cls=Analyize)

    luigi.run(["--local-scheduler"], main_task_cls=daily_analysis)
