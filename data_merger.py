import pandas as pd
import os
import glob


def merge_csv_files(input_folder, output_file_path):
    """
    Merges all CSV files in the input_folder and saves to output_file_path.
    Updated to search recursively in subfolders.
    """
    # --- UPDATED: Recursive glob to find CSVs in state subfolders ---
    search_path = os.path.join(input_folder, "**", "*.csv")
    all_files = glob.glob(search_path, recursive=True)

    if not all_files:
        return False, "No CSV files found to merge."

    all_data = []
    print(f"--- Merging {len(all_files)} files ---")

    for filename in all_files:
        try:
            # Try reading with default comma, fallback to tab if needed
            try:
                df = pd.read_csv(filename)
            except:
                df = pd.read_csv(filename, sep="\t")
            all_data.append(df)
        except Exception as e:
            print(f"Skipping {filename}: {e}")

    if not all_data:
        return False, "All CSV files were empty or unreadable."

    try:
        combined_df = pd.concat(all_data, axis=0, ignore_index=True)

        # Identify Metadata columns and Date columns
        id_cols = ["State", "RTO", "Variant", "OEM"]

        # Ensure ID columns exist
        existing_ids = [col for col in id_cols if col in combined_df.columns]

        # Identify Month Columns (starting with "20")
        month_cols = [col for col in combined_df.columns if col.startswith("20")]
        month_cols.sort()

        if not existing_ids or not month_cols:
            combined_df.to_csv(output_file_path, index=False)
            return True, f"Merged raw data (grouping skipped). Saved to {output_file_path}"

        for col in month_cols:
            combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce').fillna(0)

        # Group and Sum
        final_df = combined_df.groupby(existing_ids, as_index=False)[month_cols].sum(min_count=1)

        final_df.to_csv(output_file_path, index=False)

        # --- NEW: Also save State-wise combined files (Optional but recommended) ---
        base_output_dir = os.path.dirname(output_file_path)
        state_wise_dir = os.path.join(base_output_dir, "state_wise_combined")
        os.makedirs(state_wise_dir, exist_ok=True)

        if "State" in final_df.columns:
            for state in final_df["State"].unique():
                state_df = final_df[final_df["State"] == state]
                safe_state = str(state).replace(" ", "_")
                state_df.to_csv(os.path.join(state_wise_dir, f"{safe_state}.csv"), index=False)

        return True, f"Successfully merged {len(all_files)} files."

    except Exception as e:
        return False, f"Error during merge: {str(e)}"