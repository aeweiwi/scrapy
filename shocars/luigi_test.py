import luigi
from shocars import luigi_cars_crawler
import pandas as pd


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

if __name__ == '__main__':
    luigi.run(["--local-scheduler"], main_task_cls=Analyize)
