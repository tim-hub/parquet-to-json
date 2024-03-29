#! /usr/bin/env python
import pandas as pd
import argparse
import os
import pyarrow.orc as orc

# usage: ./converter.py --help

CONVERTERS = ['parquet2json', ]

CHAR_LIMIT=10000


def parquet2json(filename):
    df = pd.read_parquet(f'{filename}.parquet')
    df.drop('__index_level_0__', axis=1, inplace=True)

    df = df[(df['prompt'].str.len() < CHAR_LIMIT) & (df['completion'].str.len() < CHAR_LIMIT) & (
                df['completion'].str.len() + df['prompt'].str.len() < CHAR_LIMIT)]

    training_data = df.sample(frac=0.8, random_state=0)
    training_data.to_json(f'{filename}.jsonl', orient='records', lines=True)

    validation_data = df.drop(training_data.index)
    validation_data.to_json(f'{filename}_validation.jsonl', orient='records', lines=True)


def convert(args):
    converter = args.converter
    filename = os.path.splitext(args.sourcefile)[0]

    switcher = {
        "parquet2json": parquet2json,
    }

    converter_func = switcher.get(converter)
    converter_func(filename)


def main():
    parser = argparse.ArgumentParser(description="Convert parquet to json | ocr to json | orc to parquet.")
    parser.add_argument("converter", choices=CONVERTERS)
    parser.add_argument("sourcefile")
    parser.set_defaults(func=convert)
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
