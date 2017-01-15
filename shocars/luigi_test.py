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


class to_mydb(luigi.Task):

    chunksize = 1

    # def output(self):
    #    return self.pql

    def requires(self):

        return luigi_cars_crawler()

    def to_sql_k(self, frame, name, if_exists='fail', index=True,
                 index_label=None, schema=None, chunksize=None,
                 dtype=None, **kwargs):

        table = pd.io.sql.SQLTable(name, self.pandas_sql,
                                   frame=frame, index=index,
                                   if_exists=if_exists,
                                   index_label=index_label,
                                   schema=schema,
                                   dtype=dtype, **kwargs)
        table.create()
        table.insert(chunksize)

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
        errors = 0
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
                errors += 1
                print 'duplicate found {0}\n{1}'.format(errors, e)


if __name__ == '__main__':
    # luigi.run(["--local-scheduler"], main_task_cls=Analyize)

    luigi.run(["--local-scheduler"], main_task_cls=to_mydb)
