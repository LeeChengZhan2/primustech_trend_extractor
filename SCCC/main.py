import pandas as pd
import numpy as np
import os
import sys
import traceback
import glob
from datetime import datetime, timedelta
from sqlalchemy import create_engine

# === CONFIGURATION ===
METER_LIST_FILES_PATH = r'C:\Users\PTPL\WORK\primustech_trend_extractor\SCCC\Input'
OUTPUT_ROOT_PATH = r'C:\Users\PTPL\WORK\primustech_trend_extractor\SCCC\Output'
Extract_Minustes = 1
start_date_str = "02102025"
end_date_str = "09102025"
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

        # === Batch query all tags at once to speed up ===
        try:
            safe_tags = [t.replace("'", "''") for t in tag_list]
            in_list = ", ".join([f"'{t}'" for t in safe_tags])

            minute = Extract_Minustes
            # Bucket to N-minute intervals in SQL, group and average server-side
            query = f"""
            SELECT 
                n.tagname AS [TagName],
                DATEADD(minute, (DATEDIFF(minute, 0, e.OccurredOn) / {minute}) * {minute}, 0) AS OccurredOn,
                AVG(e.[Value]) AS [Value]
            FROM ({table_union_sql}) AS e
            JOIN [SIRIUSBOOT].[dbo].[Point] AS n ON e.PointId = n.id
            WHERE n.tagname IN ({in_list})
            GROUP BY n.tagname, DATEADD(minute, (DATEDIFF(minute, 0, e.OccurredOn) / {minute}) * {minute}, 0)
            ORDER BY OccurredOn;
            """

            df_all = pd.read_sql(query, engine)
        except Exception as e:
            print(f"❌ Error querying tags batch for '{FileName}': {e}")
            traceback.print_exc()
            continue

        if df_all is None or df_all.empty:
            print(f"⚠️ No data found for file '{FileName}'")
            continue

        # Round to 2 decimals as before
        try:
            df_all['Value'] = df_all['Value'].apply(lambda x: np.floor(x * 100) / 100)
        except Exception:
            pass

        # Pivot to wide format (one column per tag)
        df_all['OccurredOn'] = pd.to_datetime(df_all['OccurredOn'])
        wide = df_all.pivot(index='OccurredOn', columns='TagName', values='Value').sort_index()
        wide.columns.name = None
        wide = wide.reset_index()

        # Forward-fill to avoid blanks where some tags have sparse intervals
        value_cols = [c for c in wide.columns if c != 'OccurredOn']
        if value_cols:
            wide[value_cols] = wide[value_cols].ffill()

        if not mergefile:
            # Save individual tag CSVs based on the pivoted, forward-filled data
            wide['Date'] = wide['OccurredOn'].dt.date
            wide['Time'] = wide['OccurredOn'].dt.time
            for tag in value_cols:
                out = wide[['OccurredOn', 'Date', 'Time', tag]].copy()
                out['TagName'] = tag
                out.rename(columns={tag: 'Value'}, inplace=True)
                safe_filename = tag.replace("\\", "_").replace("/", "_").replace(":", "_")
                tag_output_path = os.path.join(OUTPUT_ROOT_PATH, f"{safe_filename}.csv")
                out[['TagName', 'OccurredOn', 'Date', 'Time', 'Value']].to_csv(tag_output_path, index=False, encoding='utf-8')
                print(f"✅ Saved: {tag_output_path}")
        else:
            # === Final Merge File (forward-filled) ===
            merged_df = wide.copy()

            # Add separate Date and Time columns
            merged_df['Date'] = merged_df['OccurredOn'].dt.date
            merged_df['Time'] = merged_df['OccurredOn'].dt.time

            # Format OccurredOn if needed
            merged_df['OccurredOn'] = merged_df['OccurredOn'].dt.strftime('%-m/%-d/%Y %-H:%M')

            # Reorder columns
            ordered_cols = ['OccurredOn', 'Date', 'Time'] + [col for col in merged_df.columns if col not in ['OccurredOn', 'Date', 'Time']]
            merged_df = merged_df[ordered_cols]

            final_output = os.path.join(OUTPUT_ROOT_PATH, f"{FileName}_MERGED_{start_date_str}_{end_date_str}.csv")
            merged_df.to_csv(final_output, index=False, encoding='utf-8')
            print(f"✅ Merged file created: {final_output}\n")

    except Exception as e:
        print(f"❌ Error processing file '{file}': {e}")
        traceback.print_exc()

print("########################################################################")
print("✅ All files processed and saved.")
print("########################################################################")

