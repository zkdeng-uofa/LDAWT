#!/usr/bin/env python

import argparse
import pandas as pd
import numpy as np
import os
import sqlite3
import json
from math import ceil
from dataclasses import dataclass, field

@dataclass
class FilterArguments:
    """
    This class instantiates the user inputs of the script that they may input through command line or through a JSON
    """
    parquet: str = field(
        default = None,
        metadata = {"help": "name of input parquet"}
    )
    grouping_col: str = field(
        default = None,
        metadata = {"help": "name of col to group on"}
    )
    groups: str = field(
        default = None,
        metadata = {"help": "number of groups to divide into"}
    )
    output_folder: str = field(
        default = None,
        metadata = {"help": "name of output folder"}
    )

def parse_args() -> argparse.Namespace:
    """
    Parse user input arguments from command line
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--parquet', 
        type = str, 
        help = "name of input parquet"
    )
    parser.add_argument(
        '--grouping_col',
        type = str,
        help = "name of col to group on"
    )
    parser.add_argument(
        '--groups', 
        type = str, 
        help = "number of groups to divide into"
    )
    parser.add_argument(
        '--output_folder', 
        type = str, 
        help = "name of output folder"
    )
    args = parser.parse_args()

    
    # if not (args.db and args.excel and args.row_name):
    #     parser.error("--db, --excel, and --row_name are required if not using --config_json")
    return FilterArguments(
        parquet=args.parquet,
        grouping_col=args.grouping_col, 
        groups=args.groups, 
        output_folder=args.output_folder
    )

def greedy_grouping(num_partitions, df, count, name):
    """
    Performs greedy grouping given a dataframe and number of partitions.
    """
    # Sort the dataframe by count in descending order to handle larger groups first
    sorted_df = df.sort_values(by=count, ascending=False).reset_index(drop=True)

    # Initialize partition data structures
    partitions = [[] for _ in range(num_partitions)]
    partition_sums = [0 for _ in range(num_partitions)]
    
    for _, row in sorted_df.iterrows():
        # Find the partition with the minimum sum that can accommodate the current row
        min_partition_idx = np.argmin(partition_sums)
        partitions[min_partition_idx].append(row)
        partition_sums[min_partition_idx] += row[count]

    # Flatten the partitions and create a new dataframe
    new_rows = []
    for group_id, partition in enumerate(partitions, 1):
        for row in partition:
            new_rows.append({name: row[name], count: row[count], "group": group_id})

    output_df = pd.DataFrame(new_rows)

    return output_df

def partition_df(df, num_partitions, taxon_col):
    # Calculate the approximate size for each partition
    partition_size = ceil(len(df) / num_partitions)

    # Sort the dataframe by the taxon column
    sorted_df = df.sort_values(by=taxon_col).reset_index(drop=True)

    # Assign each row to a partition
    sorted_df['group_num'] = ((np.arange(len(sorted_df)) // partition_size) + 1)

    # Concatenate the group number to the taxon name
    sorted_df[taxon_col] = sorted_df[taxon_col] + sorted_df['group_num'].astype(str)

    # Drop the temporary 'group_num' column
    sorted_df = sorted_df.drop(columns=['group_num'])

    return sorted_df

def main():
    inputs = parse_args()
    parquet_path = inputs.parquet
    total_df = pd.read_parquet(parquet_path)

    # Partition the DataFrame
    total_df = partition_df(total_df, int(inputs.groups), inputs.grouping_col)

    # Group by specific row and count
    count_df = total_df.groupby(inputs.grouping_col).size().reset_index(name="Count").sort_values(by='Count', ascending=False)
    groups_df = greedy_grouping(int(inputs.groups), count_df, "Count", inputs.grouping_col)

    #print("test")
    #print(groups_df.head())

    # Merge total_df with groups_df
    total_df = total_df.drop(columns=['group'], errors='ignore')
    total_df_merged = total_df.merge(groups_df[[inputs.grouping_col, "group"]], on=inputs.grouping_col, how="left")
    #print("test2")
    #print(total_df_merged.head())
    # Create output directory
    os.makedirs(inputs.output_folder, exist_ok=True)

    # Output partitioned data to parquet files
    # for group in total_df_merged["group"].unique():
    #     subset_df = total_df_merged[total_df_merged["group"] == group]
    #     print(f"{inputs.output_folder}/group_{group}.csv")
    #     subset_df.to_csv(f"{inputs.output_folder}/group_{group}.csv", index=False)

    for group in total_df_merged["group"].unique():
        subset_df = total_df_merged[total_df_merged["group"] == group]
        print(f"{inputs.output_folder}/group_{group}.parquet")
        subset_df.to_parquet(f"{inputs.output_folder}/group_{group}.parquet", index=False)

if __name__ == "__main__":
    main()