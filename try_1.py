import csv
import pandas as pd
import pyarrow.parquet as pq


class Converter:

    def convert_csv_to_parquet(self, use, infile, outfile=''):
        """Convert csv file to parquet"""
        if use:
            try:
                data = pd.read_csv(infile, sep=';')
                data.set_index(list(data.columns[[0]]), inplace=True)
            except Exception:
                print("Couldn't open the file")
            else:
                try:
                    data.to_parquet(outfile)
                except Exception:
                    print("Couldn't convert the file")
        else:
            if outfile:
                outfile = outfile.split('.')[0] + '.parquet'
            else:
                outfile = infile.split('.')[0] + '.parquet'
            try:
                data = pd.read_csv(infile, sep=';')
                data.set_index(list(data.columns[[0]]), inplace=True)
            except Exception:
                print("Couldn't open the csv file")
            else:
                try:
                    data.to_parquet(outfile)
                except Exception:
                    print("Couldn't convert the file")

    def convert_parquet_to_csv(self, use, infile, outfile=''):
        """Convert csv file to parquet"""
        if use:
            try:
                data = pd.read_parquet(infile)
            except Exception:
                print("Couldn't open the file")
            else:
                try:
                    data.to_csv(outfile)
                except Exception:
                    print("Couldn't convert the file")
        else:
            if outfile:
                outfile = outfile.split('.')[0] + '.csv'
            else:
                outfile = infile.split('.')[0] + '.csv'
            try:
                data = pd.read_parquet(infile)
            except Exception:
                print("Couldn't open the parquet file")
            else:
                try:
                    data.to_csv(outfile)
                except Exception:
                    print("Couldn't convert the file")


    def get_parquet_schema(self, file_name):
        """Get the schema of a parquet file"""

        try:
            data = pq.read_table(file_name)
        except:
            print("Couldn't open the parquet file")
        else:
            schema = f'{file_name} schema (rows={data.num_rows}, cols={data.num_columns}):\n'
            for n, t in zip(data.schema.names, data.schema.types):
                schema += f'{n}: {t}\n'
            return schema
