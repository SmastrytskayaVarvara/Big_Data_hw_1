from argparse import ArgumentParser
from try_1 import Converter


def get_arguments():
    """Get arguments from the command line"""
    
    parser = ArgumentParser(description='csv <-> parquet converter')
    parser.add_argument('-i', '--infile', type=str, required=True, help='full name of the file to convert')
    parser.add_argument('-o', '--outfile', type=str, default='', help='name of the converted file')
    parser.add_argument('-u', '--use', action='store_true', help='use the provided files for input and output')

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-tocsv', '--parquet_to_csv', action='store_true', help='convert a parquet file to csv')
    group.add_argument('-toparquet', '--csv_to_parquet', action='store_true', help='convert a csv file to parquet')
    group.add_argument('-schema', '--schema', action='store_true', help='show the schema of a parquet file')
    arguments = parser.parse_args()

    return arguments


def main():
    arguments = get_arguments()

    
    convert = Converter()
    

    if arguments.parquet_to_csv:
        convert.convert_parquet_to_csv(arguments.use, arguments.infile, outfile=arguments.outfile)
    elif arguments.csv_to_parquet:
        convert.convert_csv_to_parquet(arguments.use, arguments.infile, outfile=arguments.outfile)
    elif arguments.schema:
        print(convert.get_parquet_schema(arguments.infile))
    else:
        print('Argument is required')


if __name__ == '__main__':
    main()
