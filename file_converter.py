import os
import re
import pandas as pd
import logging
from datetime import datetime

# ==========================================
#  USER CONFIGURATION
# ==========================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Default paths
DEFAULT_INPUT_FOLDER = os.path.join(BASE_DIR, 'downloads')
DEFAULT_INTERMEDIATE_FOLDER = os.path.join(BASE_DIR, 'processed_csv')
DEFAULT_FINAL_OUTPUT = os.path.join(BASE_DIR, 'final_output', 'FINAL_MERGED_OUTPUT.csv')

# Ensure folders exist
os.makedirs(DEFAULT_INPUT_FOLDER, exist_ok=True)
os.makedirs(DEFAULT_INTERMEDIATE_FOLDER, exist_ok=True)
os.makedirs(os.path.dirname(DEFAULT_FINAL_OUTPUT), exist_ok=True)


# ==========================================
#  HELPER FUNCTIONS
# ==========================================

def setup_logging(output_folder):
    """Setup logging to file and console."""
    log_file = os.path.join(output_folder, f'processing_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt')
    logging.basicConfig(
        level=logging.INFO,
        format='%(message)s',
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()]
    )
    return log_file


def get_month_dates_for_year(year):
    """Generate month dates based on the year."""
    if not year: year = "2024"
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
    return dates.get(str(year), dates["2024"])


KNOWN_STATES = {
    "uttar_pradesh": "Uttar Pradesh",
    "maharashtra": "Maharashtra",
    "rajasthan": "Rajasthan",
    "chhattisgarh": "Chhattisgarh",
    "cg": "Chhattisgarh",
    "jharkhand": "Jharkhand",
    "madhya_pradesh": "Madhya Pradesh",
    "bihar": "Bihar",
    "punjab": "Punjab",
    "uttarakhand": "Uttarakhand",
    "arunachal_pradesh": "Arunachal Pradesh",
    "himachal_pradesh": "Himachal Pradesh",
    "jammu_kashmir": "Jammu & Kashmir",
    "assam": "Assam",
    "manipur": "Manipur",
    "delhi": "Delhi"
}


def extract_info_smart(filename):
    """
    Universally detects State, RTO, and Year from the filename.
    """
    lower_name = filename.lower()
    base = filename.rsplit('.', 1)[0]
    parts = base.split('_')

    # 1. Detect Year
    year = next((p for p in parts if re.match(r'202\d', p)), "2024")

    # 2. Detect State
    detected_key = None
    state_display = "Unknown State"

    for key, display_name in KNOWN_STATES.items():
        if lower_name.startswith(key + "_") or lower_name.startswith(key + "."):
            detected_key = key
            state_display = display_name
            break

    # 3. Detect RTO
    rto = "Unknown RTO"
    if detected_key:
        remaining = base[len(detected_key):].strip('_')
        if year in remaining:
            rto_part = remaining.split(year)[0].strip('_')
            rto = rto_part
        else:
            rto = remaining.split('_')[0]
    else:
        state_display = "Other"
        rto = parts[0] if parts else "Unknown"

    return rto.strip(), year, state_display


def process_excel_file(filepath, rto, variant, year, state_name):
    """Reads Excel and converts to structured DataFrame."""
    try:
        df = pd.read_excel(filepath, header=None)

        if df.shape[0] < 5:
            return None

        oem_col = df.iloc[4:, 1].reset_index(drop=True)
        available_cols = df.shape[1]

        max_month_cols =min(12, available_cols - 2)
        month_data = df.iloc[4:, 2:2 + max_month_cols].reset_index(drop=True)

        out_df = pd.DataFrame()
        num_rows = len(oem_col)

        out_df['State'] = [state_name] * num_rows
        out_df['RTO'] = [rto] * num_rows
        out_df['Variant'] = [variant] * num_rows
        out_df['OEM'] = oem_col

        month_dates = get_month_dates_for_year(year)

        for i, mdate in enumerate(month_dates):
            if i < month_data.shape[1]:
                out_df[mdate] = month_data.iloc[:, i].values
            else:
                out_df[mdate] = 0

        return out_df
    except Exception as e:
        logging.error(f"Error processing {os.path.basename(filepath)}: {str(e)}")
        return None


# ==========================================
#  MAIN PIPELINE FUNCTION
# ==========================================

def run_conversion_pipeline(input_folder=DEFAULT_INPUT_FOLDER, output_folder=DEFAULT_INTERMEDIATE_FOLDER):
    """
    Main entry point called by app.py.
    """
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    setup_logging(output_folder)

    logging.info(f"Scanning for files in {input_folder}...")

    processed_count = 0
    total_files = 0

    # --- UPDATED: Walk through subdirectories (Recursive Search) ---
    for root, dirs, files in os.walk(input_folder):

        valid_files = [f for f in files if f.lower().endswith(('.xlsx', '.xls', '.xlxs')) and not f.startswith('~$')]

        for fname in valid_files:
            total_files += 1
            fpath = os.path.join(root, fname)

            # Extract Info
            rto, year, state = extract_info_smart(fname)

            # Determine Variant
            base_name = fname.rsplit('.', 1)[0]
            variant = base_name.split('_')[-1].strip()

            logging.info(f"Processing: {fname} -> State: {state}")

            out_df = process_excel_file(fpath, rto, variant, year, state)

            if out_df is not None and not out_df.empty:
                # --- UPDATED: Save to State Subfolder ---
                state_clean_folder = state.replace(" ", "_")
                state_output_dir = os.path.join(output_folder, state_clean_folder)
                os.makedirs(state_output_dir, exist_ok=True)

                out_name = base_name + '.csv'
                out_df.to_csv(os.path.join(state_output_dir, out_name), index=False)
                processed_count += 1
            else:
                logging.warning(f"Failed: {fname}")

    logging.info(f"Finished. Total: {total_files}, Processed: {processed_count}")
    return processed_count, total_files


if __name__ == "__main__":
    count, total = run_conversion_pipeline()
    print(f"✅ Converted {count}/{total} files.")
