import pandas as pd
import numpy as np
import os
import sys
import traceback
import glob
from datetime import datetime, timedelta
from sqlalchemy import create_engine

# === CONFIGURATION ===
METER_LIST_FILES_PATH = r'C:\Users\PTPL\WORK\SCCC\primustech_sccc_trend_extractor\Input'
OUTPUT_ROOT_PATH = r'C:\Users\PTPL\WORK\SCCC\primustech_sccc_trend_extractor\Output'
Extract_Minustes = 1
start_date_str = "01012025"
end_date_str = "01062025"
mergefile = True

# === Convert date strings to datetime ===
start_date = datetime.strptime(start_date_str, "%d%m%Y")
end_date = datetime.strptime(end_date_str, "%d%m%Y") 

# === Loop over each Excel tag list file ===
for file in os.listdir(METER_LIST_FILES_PATH):
    if not file.endswith('.xlsx'):
        continue

    try:
        input_file_path = os.path.join(METER_LIST_FILES_PATH, file)
        FileName = file.replace(".xlsx", "")
        # output_dir = os.path.join(OUTPUT_ROOT_PATH, FileName)
        # output_dir = OUTPUT_ROOT_PATH
        # os.makedirs(output_dir, exist_ok=True)
        os.makedirs(OUTPUT_ROOT_PATH, exist_ok=True)

        # === Database Connection ===
        server = '192.168.30.12'
        database = 'SIRIUSBOOT_DATA'
        username = 'sa'
        password = 'Team2Work'
        connection_string = (
            f"mssql+pyodbc://{username}:{password}@{server}/{database}"
            "?driver=ODBC+Driver+17+for+SQL+Server"
        )
        engine = create_engine(connection_string)
        print(f"\n✅ Connected to database for: {FileName}\n")

        # === Load tag names ===
        tag_list = pd.read_excel(input_file_path, header=None)[0].tolist()
        print(f"✅ {len(tag_list)} tags loaded from {file}")

        # === Build UNION SQL for all daily tables ===
        table_union_sql = "\nUNION ALL ".join([
            f"SELECT * FROM [SIRIUSTREND].[dbo].[TBL_{(start_date + timedelta(days=i)).strftime('%d%m%Y')}]"
            for i in range((end_date - start_date).days + 1)
        ])

        # === Main Tag Loop ===
        merged_df = None

        for idx, tag in enumerate(tag_list):
            print(f"🔍 [{idx+1}/{len(tag_list)}] Processing tag: {tag}")
            safe_tag = tag.replace("'", "''")

            query = f"""
            SELECT 
                n.Name AS [TagName],
                e.[Value],
                e.OccurredOn
            FROM ({table_union_sql}) AS e
            JOIN [SIRIUSBOOT].[dbo].[Point] AS n ON e.PointId = n.id
            WHERE n.tagname = '{safe_tag}'
            ORDER BY e.PointId, e.OccurredOn;
            """

            try:
                df = pd.read_sql(query, engine)
            except Exception as e:
                print(f"❌ Error querying tag '{tag}': {e}")
                traceback.print_exc()
                continue

            if df.empty:
                print(f"⚠️ No data found for '{tag}'")
                continue

            try:
                df['OccurredOn'] = pd.to_datetime(df['OccurredOn']).dt.floor(f'{Extract_Minustes}min')
                df = df.groupby(['TagName', 'OccurredOn'], as_index=False)['Value'].mean()
                df['Value'] = df['Value'].apply(lambda x: np.floor(x * 100) / 100)

                # Prepare for merge
                tag_df = df[['OccurredOn', 'Value']].copy()
                tag_df.rename(columns={'Value': tag}, inplace=True)

                if merged_df is None:
                    merged_df = tag_df
                else:
                    merged_df = pd.merge(merged_df, tag_df, on='OccurredOn', how='outer')

                # Save only if mergefile is False
                if not mergefile:
                    df['Date'] = df['OccurredOn'].dt.date
                    df['Time'] = df['OccurredOn'].dt.time
                    df = df[['TagName', 'OccurredOn', 'Date', 'Time', 'Value']]
                    safe_filename = tag.replace("\\", "_").replace("/", "_").replace(":", "_")
                    # tag_output_path = os.path.join(output_dir, f"{safe_filename}.csv")
                    tag_output_path = os.path.join(OUTPUT_ROOT_PATH, f"{safe_filename}.csv")
                    df.to_csv(tag_output_path, index=False, encoding='utf-8')
                    print(f"✅ {len(df)} rows saved for '{tag}'")

            except Exception as e:
                print(f"❌ Error processing tag '{tag}': {e}")
                traceback.print_exc()
                continue

        # === Final Merge File ===
        if mergefile and merged_df is not None and not merged_df.empty:
            merged_df['OccurredOn'] = pd.to_datetime(merged_df['OccurredOn'])
            merged_df = merged_df.sort_values(by='OccurredOn')

            # Add separate Date and Time columns
            merged_df['Date'] = merged_df['OccurredOn'].dt.date
            merged_df['Time'] = merged_df['OccurredOn'].dt.time

            # Format OccurredOn if needed
            merged_df['OccurredOn'] = merged_df['OccurredOn'].dt.strftime('%-m/%-d/%Y %-H:%M')

            # Reorder columns
            ordered_cols = ['OccurredOn', 'Date', 'Time'] + [col for col in merged_df.columns if col not in ['OccurredOn', 'Date', 'Time']]
            merged_df = merged_df[ordered_cols]

            # final_output = os.path.join(output_dir, f"{FileName}_MERGED.csv")
            final_output = os.path.join(OUTPUT_ROOT_PATH, f"{FileName}_MERGED_{start_date_str}_{end_date_str}.csv")
            merged_df.to_csv(final_output, index=False, encoding='utf-8')
            print(f"✅ Merged file created: {final_output}\n")

    except Exception as e:
        print(f"❌ Error processing file '{file}': {e}")
        traceback.print_exc()

print("########################################################################")
print("✅ All files processed and saved.")
print("########################################################################")

