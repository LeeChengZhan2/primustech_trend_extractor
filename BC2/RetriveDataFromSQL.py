import pandas as pd
import numpy as np
import os
import sys
import traceback
import glob
from datetime import datetime, timedelta
from sqlalchemy import create_engine

# === CONFIGURATION ===
METER_LIST_FILES_PATH = r'C:\Users\PTPL\WORK\BC2\BC2 Trend Extract Script\ALL POWER METER TAGNAME'
OUTPUT_ROOT_PATH = r'C:\Users\PTPL\WORK\BC2\BC2 Trend Extract Script\METER_TREND_DATA'
Extract_Minustes = 5
start_date_str = "01042025"
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
        meter_file_path = os.path.join(METER_LIST_FILES_PATH, file)
        FileName = file.replace(".xlsx", "")
        output_dir = os.path.join(OUTPUT_ROOT_PATH, FileName)
        os.makedirs(output_dir, exist_ok=True)

        # === Database Connection ===
        server = '172.16.100.22'
        database = 'SIRIUSBOOT_DATA'
        username = 'sa'
        password = 'Pr1must#ch'
        connection_string = (
            f"mssql+pyodbc://{username}:{password}@{server}/{database}"
            "?driver=ODBC+Driver+17+for+SQL+Server"
        )
        engine = create_engine(connection_string)
        print(f"\n✅ Connected to database for: {FileName}\n")

        # === Load tag names ===
        tag_list = pd.read_excel(meter_file_path, header=None)[0].tolist()
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
            WHERE n.name = '{safe_tag}'
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
                    tag_output_path = os.path.join(output_dir, f"{safe_filename}.csv")
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

            final_output = os.path.join(output_dir, f"{FileName}_MERGED.csv")
            merged_df.to_csv(final_output, index=False, encoding='utf-8')
            print(f"✅ Merged file created: {final_output}\n")

    except Exception as e:
        print(f"❌ Error processing file '{file}': {e}")
        traceback.print_exc()

print("########################################################################")
print("✅ All files processed and saved.")
print("########################################################################")







##########################################################################################################
### FOR WRITE IN CSV FILE ///// Writing Continuously in One Sheet Until Full, Then Moving to Next FILE
##########################################################################################################
#2 decimals with STANDARD rounding up
#df['Value'] = df['Value'].apply(lambda x: f"{x:.2f}")

#2 decimals with ALWAYS rounding up 
#from decimal import Decimal, ROUND_CEILING #Decimal point round up
# df['Value'] = df['Value'].apply(lambda x: float(Decimal(str(x)).quantize(Decimal('0.01'), rounding=ROUND_CEILING)))


##########################################################################################################
### FOR WRITE IN CSV FILE ///// Writing Continuously in One Sheet Until Full, Then Moving to Next FILE
##########################################################################################################
# # Excel row limit for a single sheet (maximum rows in Excel)
# MAX_ROWS = 1_048_576

# # Counters to track file splitting
# sheet_counter = 1
# current_row = 0
# header_written = False

# # Function to generate output file path per sheet
# def get_output_csv_path(counter):
#     return rf'C:\Users\PTPL\Desktop\SQLTrendData\{FileName}_TrendData ({Months})_Sheet{counter}.csv'


    #    # Skip if DataFrame is empty or all NaN
    #     if not df.empty and not df.isna().all().all():
    #         rows_to_write = len(df)

    #         # If current file is full, create a new one
    #         if file_handle is None or current_row + rows_to_write > MAX_ROWS:
    #             if file_handle:
    #                 file_handle.close()  # Close previous file
    #             output_csv = get_output_csv_path(sheet_counter)
    #             file_handle = open(output_csv, mode='w', encoding='utf-8', newline='')
    #             header_written = False
    #             current_row = 0
    #             print(f"📄 New file created: {output_csv}")
    #             sheet_counter += 1
    #         try:
    #             # Write DataFrame to CSV
    #             df.to_csv(file_handle, index=False, header=not header_written)
    #             file_handle.flush()  # Write immediately to disk
    #             header_written = True
    #             current_row += rows_to_write
    #             print(f"✅ {rows_to_write} rows written for tag '{tag}'\n")
    #         except Exception as e:
    #             print(f"❌ Error writing to CSV for tag '{tag}': {e}")
    #             traceback.print_exc()
    #     else:
    #         print(f"⚠️  Skipped empty or invalid tag: '{tag}'\n")



#####################################################
### FOR WRITE IN EXCEL XLSX FILE ##########
#####################################################
#from pandas import ExcelWriter


# # Setup variables
# sheet_counter = 1
# MAX_EXCEL_ROWS = 1048576
# current_row = 0

##########################################################################################################
############ Writing Continuously in One Sheet Until Full, Then Moving to Next Sheet #####################
##########################################################################################################
    # if not df.empty and not df.isna().all().all():
    #     mode = "a" if os.path.exists(OUTPUT_EXCEL_PATH) else "w"
    #     writer_args = {"path": OUTPUT_EXCEL_PATH,"engine": "openpyxl","mode": mode}

    #     if mode == "a":
    #         # add the "if_sheet_exists" into the writer_args
    #         writer_args["if_sheet_exists"] = "overlay"

    #     with ExcelWriter(**writer_args) as writer:
    #         sheet_name = f"Sheet{sheet_counter}"

    #         # If adding this df exceeds the Excel row limit, go to a new sheet
    #         if current_row + len(df) > MAX_EXCEL_ROWS:
    #             sheet_counter += 1
    #             sheet_name = f"Sheet{sheet_counter}"
    #             current_row = 0

    #         print(f"Writing to {sheet_name} starting at row {current_row}...")
    #         df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=current_row)

    #         current_row += len(df) + 1  # Add +1 to avoid overlap with header next time

##########################################################################################################
############################## FOR WRITE THE EVERY SHEET FOR EACH TAGNAME ################################
##########################################################################################################
# with ExcelWriter(OUTPUT_EXCEL_PATH, engine="openpyxl", mode="a" if os.path.exists(OUTPUT_EXCEL_PATH) else "w") as writer:
#     if total_rows <= MAX_EXCEL_ROWS:
#         sheet_name = f"sheet{sheet_counter}"
#         print(f"Writing to {sheet_name}...")
#         df.to_excel(writer, index=False, sheet_name=sheet_name)
#         sheet_counter += 1
#     else:
#         num_parts = (total_rows // MAX_EXCEL_ROWS) + 1
#         for i in range(num_parts):
#             start = i * MAX_EXCEL_ROWS
#             end = start + MAX_EXCEL_ROWS
#             part_df = df.iloc[start:end]
#             sheet_name = f"sheet{sheet_counter}"
#             print(f"Writing to {sheet_name}...")
#             part_df.to_excel(writer, index=False, sheet_name=sheet_name)
#             sheet_counter += 1