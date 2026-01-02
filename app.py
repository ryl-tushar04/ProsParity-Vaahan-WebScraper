import streamlit as st
import json
import os
import shutil  # Moved to top for better practice
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# --- IMPORT MODULES ---
try:
    import main as scraper_module
except ImportError:
    st.error("Could not import main.py. Make sure it is in the same directory.")

try:
    import file_converter
    import data_merger
    import email_notifier
except ImportError:
    st.error("Could not import helper modules. Check file_converter.py, data_merger.py, and email_notifier.py")

# --- PATH CONFIGURATION ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOADS_DIR = os.path.join(BASE_DIR, "downloads")
PROCESSED_DIR = os.path.join(BASE_DIR, "processed_csv")
OUTPUT_DIR = os.path.join(BASE_DIR, "final_output")
FINAL_CSV_NAME = "Final_Merged_Vahan_Data.csv"
FINAL_CSV_PATH = os.path.join(OUTPUT_DIR, FINAL_CSV_NAME)

# Create directories
for d in [DOWNLOADS_DIR, PROCESSED_DIR, OUTPUT_DIR]:
    os.makedirs(d, exist_ok=True)

# --- CONFIG FILES ---
STATES_YEAR_FILE = 'states_and_year.json'
USER_CONFIG_FILE = 'user_config.json'
AVAILABLE_PRODUCTS = ["E2W", "L3G", "L3P", "L5G", "L5P", "ICE"]


def load_json(filepath):
    if os.path.exists(filepath):
        with open(filepath, 'r') as f:
            return json.load(f)
    return {}


def save_json(filepath, data):
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)


def main():
    st.set_page_config(page_title="Vahan Automation Pipeline", layout="wide")
    st.title("Vahan Data Automation Pipeline")
    st.markdown("### Config -> Scrape -> Process -> Merge -> Email")

    # 1. LOAD CONFIG DATA
    states_data = load_json(STATES_YEAR_FILE)
    user_config = load_json(USER_CONFIG_FILE)

    available_states = list(states_data.get("states", {}).keys())
    available_years = list(states_data.get("years", {}).keys())

    # Defaults
    def_states = user_config.get("states_to_scrape", [])
    def_years = user_config.get("years_to_scrape", [])
    def_products = user_config.get("products_to_scrape", [])

    # --- UI SECTION ---
    with st.sidebar:
        st.header("⚙️ Configuration")
        selected_states = st.multiselect("Select States", available_states,
                                         default=[s for s in def_states if s in available_states])
        selected_years = st.multiselect("Select Years", available_years,
                                        default=[y for y in def_years if y in available_years])
        selected_products = st.multiselect("Select Products", AVAILABLE_PRODUCTS,
                                           default=[p for p in def_products if p in AVAILABLE_PRODUCTS])

        st.info("Leave RTO list empty to scrape ALL RTOs.")
        rto_input = st.text_area("Specific RTOs (Optional, comma separated)", value="")

        st.divider()
        st.header("📧 Notification")
        recipient_email = st.text_input("Enter Email for Results (Optional)")
        if recipient_email and not os.environ.get("SENDER_EMAIL"):
            st.warning("⚠️ Sender credentials not found in environment. Email may fail.")

        st.divider()
        st.header("🧹 Maintenance")
        if st.button("🗑️ Clear All Previous Data", type="secondary"):
            # Define folders to clear
            folders_to_clear = [DOWNLOADS_DIR, PROCESSED_DIR, OUTPUT_DIR]

            for folder in folders_to_clear:
                if os.path.exists(folder):
                    for filename in os.listdir(folder):
                        file_path = os.path.join(folder, filename)
                        try:
                            if os.path.isfile(file_path) or os.path.islink(file_path):
                                os.unlink(file_path)
                            elif os.path.isdir(file_path):
                                shutil.rmtree(file_path)
                        except Exception as e:
                            st.error(f"Failed to delete {file_path}. Reason: {e}")

            # Also clear progress.json to force re-scrape
            if os.path.exists("progress.json"):
                os.remove("progress.json")

            st.toast("✅ All old data cleared!", icon="🧹")

    # --- PIPELINE CONTROLS ---
    st.subheader("Pipeline Controls")
    col1, col2 = st.columns([1, 2])
    with col1:
        start_btn = st.button("▶START FULL PIPELINE", type="primary", use_container_width=True)

    status_area = st.empty()
    log_area = st.expander("Processing Logs", expanded=True)

    if start_btn:
        # Save Basic Config
        new_rtos = [x.strip() for x in rto_input.split(",") if x.strip()]

        # ---------------------------------------------------------
        # 🧠 SMART ARCHIVE LOGIC
        # ---------------------------------------------------------

        # 1. Separate "Archive Years" from "Live Years"
        years_to_scrape_live = []
        use_archive_2024 = False

        for year in selected_years:
            if year == "2024":
                use_archive_2024 = True
            else:
                years_to_scrape_live.append(year)

        # 2. Update Config for the Scraper (Only give it the LIVE years)
        config_data = {
            "states_to_scrape": selected_states,
            "years_to_scrape": years_to_scrape_live,  # <--- Only 2025, etc.
            "products_to_scrape": selected_products,
            "rto_filter_list": new_rtos
        }
        save_json(USER_CONFIG_FILE, config_data)
        status_area.info("Configuration saved.")

        # 3. RUN SCRAPER (Only for Live Data)
        try:
            if len(years_to_scrape_live) > 0:
                with log_area:
                    st.write(f"🕷️ Scraping Live Data for: {years_to_scrape_live}...")
                    if hasattr(scraper_module, 'main'):
                        scraper_module.main()
                    st.write("✅ Live Scraping Completed.")
            else:
                status_area.info("⚡ Skipping Scraper (Data exists in Archive)")
        except Exception as e:
            st.error(f"❌ Scraping Failed: {e}")
            return

        # 4. INJECT ARCHIVE DATA
        if use_archive_2024:
            archive_dir = os.path.join(BASE_DIR, "archive_2024")
            if os.path.exists(archive_dir):
                status_area.info("📂 Injecting 2024 Historical Data from subfolders...")
                file_count = 0

                # os.walk goes into every subfolder (E2W_CG, etc.) recursively
                for root, dirs, files in os.walk(archive_dir):
                    for filename in files:
                        # Only copy Excel files, ignore others
                        if filename.lower().endswith(('.xlsx', '.xls')):
                            src = os.path.join(root, filename)
                            dst = os.path.join(DOWNLOADS_DIR, filename)

                            try:
                                shutil.copy2(src, dst)
                                file_count += 1
                            except Exception as e:
                                st.warning(f"Could not copy {filename}: {e}")

                if file_count > 0:
                    st.toast(f"✅ Added {file_count} historical files from 2024 archive.", icon="📜")
                else:
                    st.warning("⚠️ Found 'archive_2024' folder but it contained no Excel files!")
            else:
                st.warning("⚠️ User selected 2024, but 'archive_2024' folder was not found!")

        # 5. RUN CONVERTER (Processes BOTH Live + Archive files)
        try:
            status_area.info("🔄 Converting All Files (Live + Historical)...")
            with log_area:
                st.write(f"📂 Reading from: {DOWNLOADS_DIR}")
                converted_count, total_files = file_converter.run_conversion_pipeline(DOWNLOADS_DIR, PROCESSED_DIR)
                st.write(f"✅ Conversion Done: {converted_count}/{total_files} files processed.")
        except Exception as e:
            st.error(f"❌ Conversion Failed: {e}")
            return

        # 6. RUN MERGER
        try:
            status_area.info("🔗 Merging CSV files...")
            with log_area:
                success, msg = data_merger.merge_csv_files(PROCESSED_DIR, FINAL_CSV_PATH)
                if success:
                    st.write(f"✅ {msg}")

                    # --- EMAIL SENDING ---
                    if recipient_email:
                        st.write(f"📧 Sending email to {recipient_email}...")
                        email_success, email_msg = email_notifier.send_csv_via_email(recipient_email, FINAL_CSV_PATH)
                        if email_success:
                            st.write(email_msg)
                            status_area.success("🎉 Pipeline Finished & Email Sent!")
                        else:
                            st.error(email_msg)
                            status_area.warning("Pipeline finished, but email failed.")
                    else:
                        status_area.success("🎉 Pipeline Finished Successfully! (No email sent)")
                else:
                    st.error(f"❌ Merge Error: {msg}")
                    return
        except Exception as e:
            st.error(f"❌ Merge Failed: {e}")
            return

    # --- DOWNLOAD SECTION ---
    if os.path.exists(FINAL_CSV_PATH):
        st.divider()
        st.subheader("📥 Download Results")
        with open(FINAL_CSV_PATH, "rb") as f:
            csv_data = f.read()
            st.download_button(
                label="Download Final Merged CSV",
                data=csv_data,
                file_name=FINAL_CSV_NAME,
                mime="text/csv",
                type="primary"
            )


if __name__ == "__main__":
    main()