from cricguru import player
import pandas as pd
import os
import time
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import sys

THREADS = 16

# Get the current file path
current_file = os.path.abspath(__file__)

# Start time for performance tracking
start_time = time.perf_counter()

# Function to get stats
def get_stats(id, stat_type):
    try:
        _player = player.Player(id)
        query_params = {
            'class': '6',
            'type': stat_type
        }

        # Fetch the player career summary
        _player.career_summary(query_params, id)

        # Construct the file path for the CSV file
        file = os.path.join(os.path.dirname(current_file), f"{id}.csv")



        # Load the CSV into a DataFrame
        career_summary = pd.read_csv(file)

        # Filter for IPL stats
        ipl_stats = career_summary[career_summary['Grouping'] == 'IPL']

        # Remove the file after processing
        if os.path.exists(file):
            os.remove(file)  # Correct way to remove a file
        else:
            print("File not found.")
    except Exception as e:
        print(f"Error fetching stats for player {id}, type: {stat_type}")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        line_number = exc_tb.tb_lineno
        print(f"\033[91mError on line {line_number}\033[0m")
        print(e)
        ipl_stats = pd.DataFrame()


    return ipl_stats

# Define roles mapping to player stats
role = {
    "batsman": "batting",
    "bowler": "bowling",
    "all-rounder": "allround",
    "wk/batsman": "batting",
}

# Define player stats categories
player_stats_columns = {
    "fielding": ["Ct", "St", "Mat"],
    "batting": ["Ave", "SR", "100", "50", "4s", "6s", "BoundaryPercentage"],
    "bowling": ["BAve", "Econ", "BSR", "Wkts"]

}

# Main function to process player stats
def process_player_stats(row):
    try:
        player_id = row['id']
        player_role = row['role']
        player_name = row['name']
        player_type = role.get(player_role, "batting")  # Default to batting if role not found

        ipl_df = pd.DataFrame()
        fielding_df = pd.DataFrame()
        batting_df = pd.DataFrame()
        bowling_df = pd.DataFrame()
        # Get batting/bowling stats
        
        if player_type == "allround":
            print(f"Fetching stats for player {player_name}, type: batting")
            batting_df = get_stats(player_id, "batting")
            print(f"Fetching stats for player {player_name}, type: bowling")
            bowling_df = get_stats(player_id, "bowling")

        else:
            print(f"Fetching stats for player {player_name}, type: {player_type}")
            ipl_df = get_stats(player_id, player_type)
        
            # Get fielding stats
        print(f"Fetching stats for player {player_name}, type: felding")
        fielding_df = get_stats(player_id, "fielding")


        # Update batting/bowling stats-



        if not ipl_df.empty and not fielding_df.empty or not batting_df.empty and not bowling_df.empty:
            if player_type == "allround":
                print(batting_df.columns)
                print(bowling_df.columns)
                
                for stat in player_stats_columns["batting"]:
                    if stat in batting_df.columns:
                        row[stat] = batting_df[stat].values[0]
                    if stat == "BoundaryPercentage" and 'BF' in batting_df.columns and batting_df['BF'].values[0] != 0:
                        row[stat] = round(float((float(batting_df['4s'].values[0]) + float(batting_df['6s'].values[0])) / float(batting_df['BF'].values[0]) * 100),2)
                        print("row[stat]", row[stat])

                row['Wkts'] = bowling_df['Wkts'].values[0]
                row['BAve'] = bowling_df['Ave'].values[0]
                row['Econ'] = bowling_df['Econ'].values[0]
                row['BSR'] = bowling_df['SR'].values[0]

                # # bowling stats 
                # print(ipl_df.columns)
                # row['Avg'] = ipl_df['Bat Av'].values[0]
                

       
            elif player_type == "batting":
                print("batting", ipl_df.columns)
                for stat in player_stats_columns[player_type]:
                    if stat in ipl_df.columns:
                        row[stat] = ipl_df[stat].values[0]
                    if stat == "BoundaryPercentage" and 'BF' in ipl_df.columns and ipl_df['BF'].values[0] != 0:
                        row[stat] = round(float((float(ipl_df['4s'].values[0]) + float(ipl_df['6s'].values[0])) / float(ipl_df['BF'].values[0]) * 100),2)
                        print("row[stat]", row[stat])
            
            elif player_type == "bowling":
                row['Wkts'] = ipl_df['Wkts'].values[0]
                row['BAve'] = ipl_df['Ave'].values[0]
                row['Econ'] = ipl_df['Econ'].values[0]
                row['BSR'] = ipl_df['SR'].values[0]
                
             # Update fielding stats
            for stat in player_stats_columns["fielding"]:
                if stat in fielding_df.columns:
                    row[stat] = fielding_df[stat].values[0]

        print(f"Processed {player_name}, {player_role}")
        return row
    except Exception as e:
        print(f"Error processing player {player_name}")
        print(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        line_number = exc_tb.tb_lineno
        print(f"\033[91mError on line {line_number}\033[0m")
        return []
    

# Main function to execute the program
def main():
    player_stats_dataset = os.path.join(os.path.dirname(current_file), 'player_dataset.csv')
    player_df = pd.read_csv(player_stats_dataset)

    # Initialize new columns for stats in the player DataFrame
    for stat_list in player_stats_columns.values():
        for stat in stat_list:
            player_df[stat] = 0

    # # Process players concurrently using ThreadPoolExecutor
    # with ThreadPoolExecutor(max_workers=THREADS) as executor:
    #     processed_rows = list(executor.map(process_player_stats, [row for _, row in player_df.iterrows()]))
    processed_rows = []
    for _, row in player_df.iterrows():
        processed_rows.append(process_player_stats(row))

    # Convert the processed rows back into a DataFrame
    processed_df = pd.DataFrame(processed_rows)

    # Save the updated DataFrame back to CSV
    output_file = os.path.join(os.path.dirname(current_file), 'updated_player_dataset.csv')
    processed_df.to_csv(output_file, index=False)

    print(f"Processed player stats saved to {output_file}")

if __name__ == "__main__":
    main()

end = time.perf_counter()
print(f"Time taken: {end - start_time} seconds")
