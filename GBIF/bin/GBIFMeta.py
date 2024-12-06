import dask.dataframe as dd
import argparse
from dask.diagnostics import ProgressBar
from tqdm import tqdm
import os

def parse_args():
    parser = argparse.ArgumentParser(description="Make meta information.")
    parser.add_argument("--output_name", type=str, help="Path to the output parquet file.")
    parser.add_argument("--input_folder", type=str, help="Path to the input tar file.")
    return parser.parse_args()

def main():
    # Specify the dtypes for the problematic columns in verbatimDf
    args = parse_args()
    # Read the text files into Dask DataFrames with the specified dtypes
    with ProgressBar():
        imageDf = dd.read_csv(f'{args.input_folder}/multimedia.txt', sep='\t', dtype='object')
        occurrenceDf = dd.read_csv(f'{args.input_folder}/occurrence.txt', sep='\t', dtype='object')

    columns = ["gbifID",
               "occurrenceID",
               "individualCount",
               "sex",
               "lifeStage",
               "year",
               "month",
               "day",
               "higherGeography",
               "kingdom",
               "phylum",
               "class",
               "order",
               "family",
               "genus",
               "species",
               "taxonRank"]

    # Filter imageDf to keep only rows where 'identifier' has actual values
    filtered_imageDf = imageDf[imageDf['identifier'].notnull()]

    # Select only the 'gbifID' and 'identifier' columns from the filtered imageDf
    filtered_imageDf = filtered_imageDf[['gbifID', 'identifier']]

    filtered_occurrenceDf = occurrenceDf[columns]

    # Merge the filtered imageDf with occurrenceDf on the 'gbifID' column
    resultDf = dd.merge(filtered_imageDf, filtered_occurrenceDf, on='gbifID', how='inner')

    # Compute and display the result with a progress bar
    with ProgressBar():
        result = resultDf.compute()

    # Save the result to a Parquet file with a progress bar
    #print(args.output_name)

    output_dir = 'data/parquet'
    os.makedirs(output_dir, exist_ok=True)
    with tqdm(total=1, desc="Saving to Parquet") as pbar:
        result.to_parquet(f'{output_dir}/{args.output_name}', engine='pyarrow')
        pbar.update(1)

    print("Completed successfully.")

if __name__ == "__main__":
    main()