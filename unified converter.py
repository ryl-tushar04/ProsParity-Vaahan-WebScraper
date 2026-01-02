import os
import re
import pandas as pd
import logging

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')


def get_month_dates_for_year(year):
    """Returns month-end dates based on the year."""
    dates = {
        "2022": ["2022-01-31", "2022-02-28", "2022-03-31", "2022-04-30", "2022-05-31", "2022-06-30",
                 "2022-07-31", "2022-08-31", "2022-09-30", "2022-10-31", "2022-11-30", "2022-12-31"],
        "2023": ["2023-01-31", "2023-02-28", "2023-03-31", "2023-04-30", "2023-05-31", "2023-06-30",
                 "2023-07-31", "2023-08-31", "2023-09-30", "2023-10-31", "2023-11-30", "2023-12-31"],
        "2024": ["2024-01-31", "2024-02-29", "2024-03-31", "2024-04-30", "2024-05-31", "2024-06-30",
                 "2024-07-31", "2024-08-31", "2024-09-30", "2024-10-31", "2024-11-30", "2024-12-31"],
        "2025": ["2025-01-31", "2025-02-28", "2025-03-31", "2025-04-30", "2025-05-31", "2025-06-30",
                 "2025-07-31", "2025-08-31", "2025-09-30", "2025-10-31", "2025-11-30", "2025-12-31"]
    }
    return dates.get(year, dates["2024"])


def parse_filename(filename):
    """
    Intelligently parses filenames like:
    'uttar_pradesh_Agra RTO - UP80_2025_E2W.xlsx'
    'chhattisgarh_Ambikapur RTO - CG15_2025_E2W.xlsx'
    """
    base = filename.replace('.xlsx', '')
    parts = base.split('_')

    # 1. Identify Year (Look for 4 digits)
    year = None
    year_index = -1
    for i, part in enumerate(parts):
        if re.match(r'^\d{4}$', part):
            year = part
            year_index = i
            break

    if not year or year_index == -1:
        return None, None, None, None


    product = parts[-1]

    #First part is usually state.
    state = parts[0]
    if len(parts) > year_index and parts[0] == "uttar" and parts[1] == "pradesh":
        state = "Uttar Pradesh"
        # RTO is everything between index 2 and year_index
        rto_parts = parts[2:year_index]
        rto = "_".join(rto_parts)
    elif parts[0].lower() == "chhattisgarh" or parts[0].lower() == "cg":
        state = "Chhattisgarh"
        rto_parts = parts[1:year_index]
        rto = "_".join(rto_parts)
    else:
        # Generic Fallback
        state = parts[0]
        rto_parts = parts[1:year_index]
        rto = "_".join(rto_parts)

    return state, rto, year, product


def process_single_file(filepath, output_folder):
    filename = os.path.basename(filepath)
    if not filename.endswith('.xlsx'):
        return False

    state_name, rto, year, variant = parse_filename(filename)

    if not rto or not year:
        logging.warning(f"Skipping {filename}: Could not parse RTO or Year.")
        return False

    try:
        df = pd.read_excel(filepath, header=None)

        # OEM Data starts at row 5 (index 4), Column B (index 1)
        oem_col = df.iloc[4:, 1].reset_index(drop=True)

        # Month Data starts at row 5, Column C onwards
        available_cols = df.shape[1]

        # Logic: For 2025, limit to 11 months. Else 12.
        if year == "2025":
            max_month_cols = min(11, available_cols - 2)
        else:
            max_month_cols = min(12, available_cols - 2)

        month_data = df.iloc[4:, 2:2 + max_month_cols].reset_index(drop=True)

        # Construct Output DataFrame
        out_df = pd.DataFrame()
        num_rows = len(oem_col)

        out_df['State'] = [state_name] * num_rows
        out_df['RTO'] = [rto] * num_rows
        out_df['Variant'] = [variant] * num_rows
        out_df['OEM'] = oem_col

        # Add date columns
        month_dates = get_month_dates_for_year(year)
        if year == "2025":
            month_dates = month_dates[:11]

        for i, mdate in enumerate(month_dates):
            if i < month_data.shape[1]:
                out_df[mdate] = month_data.iloc[:, i].values
            else:
                out_df[mdate] = 0

        # Save
        out_name = filename.replace('.xlsx', '.csv')
        out_path = os.path.join(output_folder, out_name)
        out_df.to_csv(out_path, index=False)
        return True

    except Exception as e:
        logging.error(f"Failed to process {filename}: {e}")
        return False


def run_conversion_pipeline(input_folder, output_folder):
    """Main entry point called by app.py"""
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    count = 0
    files = os.listdir(input_folder)
    total = len([f for f in files if f.endswith('.xlsx')])

    for fname in files:
        fpath = os.path.join(input_folder, fname)
        if process_single_file(fpath, output_folder):
            count += 1

    return count, total