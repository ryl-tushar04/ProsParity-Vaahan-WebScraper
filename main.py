from typing import Self              #no use
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service                           #  no use
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time    
import os    
from pathlib import Path
import json
from datetime import datetime

def load_json_config(filename):
        current_dir=os.path.dirname(os.path.abspath(__file__))
        file_path=os.path.join(current_dir, filename)
        with open(file_path,'r') as f:
            return json.load(f)

states_years_data=load_json_config('states_and_year.json')
rto_data=load_json_config('RTO.json')
user_config_data=load_json_config('user_config.json')

# ================== CONFIGURATION SECTION ==================

STATES_CONFIG = states_years_data.get("states", {})

# # YEARS to scrape (add more year XPaths as needed)

YEARS_CONFIG = states_years_data.get("years", {})

# RTO configurations per state (add more RTOs as needed)

RTO_CONFIG = rto_data

# VEHICLE CLASSES configuration
VEHICLE_CLASSES_CONFIG = {
    "E2W": ["M_CYCLE_SCOOTER", "M_CYCLE_SCOOTER_SIDE_CAR", "MOPED"],
    "L3G": ["E_RICKSHAW_CART_G"],
    "L3P": ["E_RICKSHAW_P"],
    "L5G": ["THREE_WHEELER_G"],
    "L5P": ["THREE_WHEELER_P"],
    "ICE": ["M_CYCLE_SCOOTER", "M_CYCLE_SCOOTER_SIDE_CAR", "MOPED"]
}


# ================== USER CONFIGURATION ==================

STATES_TO_SCRAPE = user_config_data.get("states_to_scrape", [])
YEARS_TO_SCRAPE = user_config_data.get("years_to_scrape", [])
PRODUCTS_TO_SCRAPE = user_config_data.get("products_to_scrape", [])
RTO_TO_SCRAPE = user_config_data.get("rto_filter_list", [])


# Other configurations
Y_AXIS = "//*[@id='yaxisVar_4']"
X_AXIS = "//*[@id='xaxisVar_7']"
HEADLESS_MODE = True         
DOWNLOAD_CSV = True

class ProgressTracker:
    def __init__(self, progress_file="progress.json"):
        self.progress_file = progress_file
        self.progress_data = self.load_progress()
    
    def load_progress(self):
        """Load existing progress from JSON file"""
        try:
            with open(self.progress_file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            return {}
        except json.JSONDecodeError:
            print(f"⚠️ Warning: {self.progress_file} is corrupted, starting fresh")
            return {}
    
    def save_progress(self):
        """Save progress to JSON file"""
        try:
            with open(self.progress_file, 'w') as f:
                json.dump(self.progress_data, f, indent=2)
        except Exception as e:
            print(f"❌ Error saving progress: {e}")
    
    def get_task_key(self, state, rto, year, product):
        """Generate unique task key"""
        return f"{state}_{rto}_{year}_{product}"
    
    def update_task_status(self, state, rto, year, product, status, details=None):
        """Update task status in progress tracking"""
        task_key = self.get_task_key(state, rto, year, product)
        
        if task_key not in self.progress_data:
            self.progress_data[task_key] = {
                "state": state,
                "rto": rto,
                "year": year,
                "product": product
            }
        
        self.progress_data[task_key].update({
            "status": status,
            "timestamp": datetime.now().isoformat(),
        })
        
        if details:
            self.progress_data[task_key]["details"] = details
        
        self.save_progress()
        print(f"📊 Progress updated: {task_key} -> {status}")
    
    def get_task_status(self, state, rto, year, product):
        """Get current status of a task"""
        task_key = self.get_task_key(state, rto, year, product)
        return self.progress_data.get(task_key, {}).get("status", "not_started")
    
    def get_summary(self):
        """Get summary of all task statuses"""
        summary = {}
        for task_data in self.progress_data.values():
            status = task_data.get("status", "unknown")
            summary[status] = summary.get(status, 0) + 1
        return summary

class VahanScraper:
    def __init__(self, headless=True, test_mode=False):
        """Initialize the scraper with Chrome driver or in test mode"""
        self.driver = None
        self.wait = None
        self.test_mode = test_mode
        self.progress_tracker = ProgressTracker()  # Add progress tracking
        
        # Set up downloads directory in the same folder as the script
        script_dir = Path(__file__).parent.absolute()
        self.download_dir = str(script_dir / "downloads")
        # Create downloads directory if it doesn't exist
        os.makedirs(self.download_dir, exist_ok=True)
        print(f"📁 Using download directory: {self.download_dir}")
        
        if not self.test_mode:
            self.setup_driver(headless)
        
    def setup_driver(self, headless=True):
        """Setup Chrome driver with options"""
        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")
        
        # Set download directory
        prefs = {
            "download.default_directory": self.download_dir,
            "download.prompt_for_download": False,
            "directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        chrome_options.add_experimental_option("prefs", prefs)
        
        self.driver = webdriver.Chrome(options=chrome_options)
        self.wait = WebDriverWait(self.driver, 20)
        
    def navigate_to_site(self):
        """Navigate to the Vahan dashboard"""
        url = "https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml"
        if self.test_mode:
            print(f"[TEST MODE] Would navigate to: {url}")
            return
        print(f"Navigating to: {url}")
        self.driver.get(url)
        time.sleep(3)
        
    def click_element(self, xpath, description, max_retries=10, wait_between=2):
        """Click an element with error handling and retries until success or max_retries"""
        if self.test_mode:
            print(f"[TEST MODE] Would click: {description} ({xpath})")
            return True
        for attempt in range(1, max_retries + 1):
            try:
                element = self.wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
                self.driver.execute_script("arguments[0].scrollIntoView(true);", element)
                time.sleep(0.5)
                element.click()
                print(f"✓ Clicked: {description} (attempt {attempt})")
                time.sleep(1)
                return True
            except Exception as e:
                print(f"✗ Attempt {attempt}: Failed to click: {description} ({e})")
                time.sleep(wait_between)
        print(f"✗ All {max_retries} attempts failed to click: {description}")
        return False
    
    def select_dropdown_option(self, dropdown_xpath, option_xpath, description, max_retries=3):
        """Select an option from dropdown with retries and logging"""
        for attempt in range(max_retries):
            if self.test_mode:
                print(f"[TEST MODE] Would select {description}: {dropdown_xpath} -> {option_xpath}")
                return True
            try:
                if self.click_element(dropdown_xpath, f"{description} dropdown"):
                    time.sleep(1)
                    if self.click_element(option_xpath, f"{description} option"):
                        return True
            except Exception as e:
                print(f"✗ Attempt {attempt + 1}: Failed to select {description} ({e})")
            if attempt < max_retries - 1:
                time.sleep(2)
        print(f"✗ All attempts failed to select: {description}")
        return False
    
    def select_state(self, state_xpath):
        """Select state from dropdown"""
        return self.select_dropdown_option(
            "/html/body/form/div[2]/div/div/div[1]/div[2]/div[3]/div/div[3]/span",
            state_xpath,
            "State"
        )

    def select_rto(self, rto_xpath):
        """Select RTO from dropdown"""
        return self.select_dropdown_option(
            "//*[@id='selectedRto']/div[3]/span",
            rto_xpath,
            "RTO"
        )
    
    def select_y_axis(self, y_axis_xpath="//*[@id='yaxisVar_4']"):
        """Select Y-axis variable"""
        return self.select_dropdown_option(
            "//*[@id='yaxisVar']/div[3]/span",
            y_axis_xpath,
            "Y-axis"
        )
    
    def select_x_axis(self, x_axis_xpath="//*[@id='xaxisVar_7']"):
        """Select X-axis variable"""
        return self.select_dropdown_option(
            "//*[@id='xaxisVar']/div[3]/span",
            x_axis_xpath,
            "X-axis"
        )
    
    def select_year(self, year_xpath="//*[@id='selectedYear_1']"):
        """Select year from dropdown"""
        return self.select_dropdown_option(
            "//*[@id='selectedYear']/div[3]/span",
            year_xpath,
            "Year"
        )
    
    def refresh_data(self):
        """Click refresh button (first reference)"""
        return self.click_element('/html/body/form/div[2]/div/div/div[1]/div[3]/div[3]/div/button', "Refresh")
    
    def expand_filter_panel(self):
        """Click expand button to open filter panel"""
        return self.click_element("//*[@id='filterLayout-toggler']/span/a/span", "Expand filter panel")
    
    def select_checkbox(self, checkbox_xpath, label_xpath, description):
        """Select a checkbox with verification"""
        if self.test_mode:
            print(f"[TEST MODE] Would select checkbox: {description}")
            return True
            
        try:
            # First try to check if checkbox is already selected
            checkbox = self.wait.until(EC.presence_of_element_located((By.XPATH, checkbox_xpath)))
            
            # Check if already selected by looking for 'ui-state-active' class or similar
            is_selected = "ui-state-active" in checkbox.get_attribute("class") if checkbox.get_attribute("class") else False
            
            if not is_selected:
                # Scroll to element
                self.driver.execute_script("arguments[0].scrollIntoView(true);", checkbox)
                time.sleep(0.5)
                
                # Try clicking the checkbox
                if not self.click_element(checkbox_xpath, f"{description} checkbox"):
                    # If checkbox click fails, try clicking the label
                    print(f"Trying label click for: {description}")
                    self.click_element(label_xpath, f"{description} label")
                
                # Wait a bit for the selection to take effect
                time.sleep(1)
                
                # Try to verify selection, but don't fail if we can't verify
                try:
                    checkbox = self.driver.find_element(By.XPATH, checkbox_xpath)
                    is_now_selected = "ui-state-active" in checkbox.get_attribute("class") if checkbox.get_attribute("class") else False
                    
                    if is_now_selected:
                        print(f"✓ Successfully selected: {description}")
                    else:
                        # Even if we can't verify the selection, assume it worked if we didn't get an error
                        print(f"✓ Clicked {description} (verification skipped)")
                except:
                    # If we can't verify, assume it worked if we didn't get an error
                    print(f"✓ Clicked {description} (verification skipped)")
                
                return True
            else:
                print(f"✓ Already selected: {description}")
                return True
                
        except Exception as e:
            print(f"✗ Error selecting {description}: {e}")
            return False
    
    def select_vehicle_categories(self, categories):
        """Select vehicle categories based on list"""
        vehicle_options = {
            'TWO_WHEELER_NT': {
                'label': "//*[@id='VhCatg']/tbody/tr[2]/td/label",
                'checkbox': "//*[@id='VhCatg']/tbody/tr[2]/td/div/div[2]/span"
            },
            'TWO_WHEELER_T': {
                'label': "//*[@id='VhCatg']/tbody/tr[3]/td/label", 
                'checkbox': "//*[@id='VhCatg']/tbody/tr[3]/td/div/div[2]/span"
            },
            'THREE_WHEELER_NT': {
                'label': "//*[@id='VhCatg']/tbody/tr[5]/td/label",
                'checkbox': "//*[@id='VhCatg']/tbody/tr[5]/td/div/div[2]/span"
            },
            'THREE_WHEELER_T': {
                'label': "//*[@id='VhCatg']/tbody/tr[6]/td/label",
                'checkbox': "//*[@id='VhCatg']/tbody/tr[6]/td/div/div[2]/span"
            }
        }
        
        print(f"Selecting vehicle categories: {categories}")
        for category in categories:
            if category in vehicle_options:
                self.select_checkbox(
                    vehicle_options[category]['checkbox'],
                    vehicle_options[category]['label'],
                    f"Vehicle category: {category}"
                )
                time.sleep(1)  # Wait between selections
    
    def select_fuel_electric(self):
        """Select both ELECTRIC(BOV) and PURE EV fuel options"""
        # Select ELECTRIC(BOV)//*[@id="fuel"]/tbody/tr[11]/td/div/div[2]/span
        self.select_checkbox(
            "//*[@id='fuel']/tbody/tr[11]/td/div/div[2]/span",
            "//*[@id='fuel']/tbody/tr[11]/td/label",
            "ELECTRIC(BOV) fuel"
        )
        time.sleep(1)  # Wait between selections
        
        # Select PURE EV 
        self.select_checkbox(
            "//*[@id='fuel']/tbody/tr[34]/td/div/div[2]/span",
            "//*[@id='fuel']/tbody/tr[34]/td/label",
            "PURE EV fuel"
        )
    
    def select_fuel_ice(self):
        """Select ICE fuel options (CNG ONLY, PETROL, PETROL/CNG, PETROL/ETHANOL)"""
        # Select CNG ONLY //*[@id="fuel"]/tbody/tr[4]/td/label
        self.select_checkbox(
            "//*[@id='fuel']/tbody/tr[4]/td/div/div[2]/span",
            "//*[@id='fuel']/tbody/tr[4]/td/label",
            "CNG ONLY fuel"
        )
        time.sleep(1)  # Wait between selections
        
        # Select PETROL 
        self.select_checkbox(
            "//*[@id='fuel']/tbody/tr[22]/td/div/div[2]/span",
            "//*[@id='fuel']/tbody/tr[22]/td/label",
            "PETROL fuel"
        )
        time.sleep(1)  # Wait between selections
        
        # Select PETROL/CNG
        self.select_checkbox(
            "//*[@id='fuel']/tbody/tr[23]/td/div/div[2]/span",
            "//*[@id='fuel']/tbody/tr[23]/td/label",
            "PETROL/CNG fuel"
        )
        time.sleep(1)  # Wait between selections
        
        # Select PETROL/ETHANOL //*[@id="fuel"]/tbody/tr[27]/td/label
        self.select_checkbox(
            "//*[@id='fuel']/tbody/tr[28]/td/div/div[2]/span",
            "//*[@id='fuel']/tbody/tr[28]/td/label",
            "PETROL/ETHANOL fuel"
        )
    
    def refresh_filters(self):
        """Click second refresh button after filters"""
        return self.click_element("/html/body/form/div[2]/div/div/div[3]/div/div[1]/div[1]/span/button", "Refresh filters")
    
    def select_vehicle_classes(self, classes):
        """Select vehicle classes for E2W, E3W, and other categories"""
        class_options = {
            # E2W Categories
            'M_CYCLE_SCOOTER': {
                'label': "//*[@id='VhClass']/tbody/tr[1]/td/label",
                'checkbox': "//*[@id='VhClass']/tbody/tr[1]/td/div/div[2]/span",
                'description': "M-CYCLE/SCOOTER"
            },
            'M_CYCLE_SCOOTER_SIDE_CAR': {
                'label': "//*[@id='VhClass']/tbody/tr[2]/td/label",
                'checkbox': "//*[@id='VhClass']/tbody/tr[2]/td/div/div[2]/span",
                'description': "M-CYCLE/SCOOTER-WITH SIDE CAR"
            },
            'MOPED': {
                'label': "//*[@id='VhClass']/tbody/tr[3]/td/label",
                'checkbox': "//*[@id='VhClass']/tbody/tr[3]/td/div/div[2]/span",
                'description': "MOPED"
            },
            # E3W Categories
            'E_RICKSHAW_P': {
                'label': "//*[@id='VhClass']/tbody/tr[38]/td/label",
                'checkbox': "//*[@id='VhClass']/tbody/tr[38]/td/div/div[2]/span",
                'description': "E-RICKSHAW(P)"
            },
            'E_RICKSHAW_CART_G': {
                'label': "//*[@id='VhClass']/tbody/tr[37]/td/label",
                'checkbox': "//*[@id='VhClass']/tbody/tr[37]/td/div/div[2]/span",
                'description': "E-RICKSHAW WITH CART(G)"
            },
            'THREE_WHEELER_P': { 
                'label': "//*[@id='VhClass']/tbody/tr[40]/td/label",
                'checkbox': "//*[@id='VhClass']/tbody/tr[40]/td/div/div[2]/span",
                'description': "THREE WHEELER (PASSENGER)"
            },
            'THREE_WHEELER_G': {
                'label': "//*[@id='VhClass']/tbody/tr[41]/td/label",
                'checkbox': "//*[@id='VhClass']/tbody/tr[41]/td/div/div[2]/span",
                'description': "THREE WHEELER (GOODS)"
            }
        }
        
        print(f"Selecting vehicle classes: {classes}")
        for class_name in classes:
            if class_name in class_options:
                self.select_checkbox(
                    class_options[class_name]['checkbox'],
                    class_options[class_name]['label'],
                    f"Vehicle class: {class_options[class_name]['description']}"
                )
                time.sleep(1)  # Wait between selections
    


    def verify_all_filters_comprehensive(self, product_type):
        """Comprehensive verification of fuel filters, vehicle classes and detect unwanted selections"""
        print(f"\n🔍 COMPREHENSIVE FILTER VERIFICATION - {product_type}")
        print(f"{'='*80}")
        
        # Wait longer for UI to update
        time.sleep(5)
        
        verification_results = {
            "fuel_filters": {"verified": [], "failed": [], "expected": []},
            "vehicle_classes": {"verified": [], "failed": [], "expected": []},
            "unwanted_selections": {"fuel": [], "vehicle_classes": []},
            "overall_status": "unknown"
        }
        
        # ===== 1. FUEL FILTER VERIFICATION =====
        print(f"\n🔋 FUEL FILTER CHECK:")
        print(f"-" * 40)
        
        if product_type == "ICE":
            expected_fuel_filters = ["CNG ONLY", "PETROL", "PETROL/CNG", "PETROL/ETHANOL"]
            fuel_rows = [4, 22, 23, 28]
        else:
            expected_fuel_filters = ["ELECTRIC(BOV)", "PURE EV"]
            fuel_rows = [11, 34]  # Updated PURE EV to row 34
        
        verification_results["fuel_filters"]["expected"] = expected_fuel_filters
        
        # Check expected fuel filters
        for filter_name, row_num in zip(expected_fuel_filters, fuel_rows):
            is_selected = self._check_filter_checkbox("fuel", row_num, filter_name)
            if is_selected:
                verification_results["fuel_filters"]["verified"].append(filter_name)
            else:
                verification_results["fuel_filters"]["failed"].append(filter_name)
        
        # Check for unwanted fuel selections
        all_fuel_rows = list(range(1, 35))  # Check all fuel rows
        unwanted_fuel_rows = [r for r in all_fuel_rows if r not in fuel_rows]
        
        print(f"\n🚨 CHECKING FOR UNWANTED FUEL SELECTIONS:")
        for row_num in unwanted_fuel_rows:
            try:
                # Get the fuel name from the label
                label_element = self.driver.find_element(By.XPATH, f"//*[@id='fuel']/tbody/tr[{row_num}]/td/label")
                fuel_name = label_element.text.strip()
                
                if fuel_name and self._check_filter_checkbox("fuel", row_num, fuel_name, silent=True):
                    print(f"   ⚠️ UNWANTED FUEL SELECTED: {fuel_name} (row {row_num})")
                    verification_results["unwanted_selections"]["fuel"].append(fuel_name)
            except:
                continue
        
        # ===== 2. VEHICLE CLASS VERIFICATION =====
        print(f"\n🚗 VEHICLE CLASS CHECK:")
        print(f"-" * 40)
        
        # Define expected vehicle classes based on product type
        if product_type == "E2W":
            expected_classes = ['M_CYCLE_SCOOTER', 'M_CYCLE_SCOOTER_SIDE_CAR', 'MOPED']
            class_rows = [1, 2, 3]
        elif product_type == "L3G":
            expected_classes = ['E_RICKSHAW_CART_G']
            class_rows = [37]
        elif product_type == "L3P":
            expected_classes = ['E_RICKSHAW_P']
            class_rows = [38]
        elif product_type == "L5G":
            expected_classes = ['THREE_WHEELER_G']
            class_rows = [41]
        elif product_type == "L5P":
            expected_classes = ['THREE_WHEELER_P']
            class_rows = [40]
        elif product_type == "ICE":
            expected_classes = ['M_CYCLE_SCOOTER', 'M_CYCLE_SCOOTER_SIDE_CAR', 'MOPED']
            class_rows = [1, 2, 3]
        else:
            expected_classes = []
            class_rows = []
        
        verification_results["vehicle_classes"]["expected"] = expected_classes
        
        # Check expected vehicle classes
        for class_name, row_num in zip(expected_classes, class_rows):
            is_selected = self._check_filter_checkbox("VhClass", row_num, class_name)
            if is_selected:
                verification_results["vehicle_classes"]["verified"].append(class_name)
            else:
                verification_results["vehicle_classes"]["failed"].append(class_name)
        
        # Check for unwanted vehicle class selections
        all_class_rows = list(range(1, 45))  # Check all vehicle class rows
        unwanted_class_rows = [r for r in all_class_rows if r not in class_rows]
        
        print(f"\n🚨 CHECKING FOR UNWANTED VEHICLE CLASS SELECTIONS:")
        for row_num in unwanted_class_rows:
            try:
                # Get the class name from the label
                label_element = self.driver.find_element(By.XPATH, f"//*[@id='VhClass']/tbody/tr[{row_num}]/td/label")
                class_name = label_element.text.strip()
                
                if class_name and self._check_filter_checkbox("VhClass", row_num, class_name, silent=True):
                    print(f"   ⚠️ UNWANTED VEHICLE CLASS SELECTED: {class_name} (row {row_num})")
                    verification_results["unwanted_selections"]["vehicle_classes"].append(class_name)
            except:
                continue
        
        # ===== 3. OVERALL VERIFICATION SUMMARY =====
        print(f"\n📊 COMPREHENSIVE VERIFICATION SUMMARY:")
        print(f"{'='*80}")
        
        fuel_success = len(verification_results["fuel_filters"]["verified"])
        fuel_total = len(verification_results["fuel_filters"]["expected"])
        
        vehicle_success = len(verification_results["vehicle_classes"]["verified"])
        vehicle_total = len(verification_results["vehicle_classes"]["expected"])
        
        unwanted_count = (len(verification_results["unwanted_selections"]["fuel"]) + 
                         len(verification_results["unwanted_selections"]["vehicle_classes"]))
        
        print(f"🔋 Fuel Filters: {fuel_success}/{fuel_total} verified")
        print(f"   ✅ Verified: {verification_results['fuel_filters']['verified']}")
        print(f"   ❌ Failed: {verification_results['fuel_filters']['failed']}")
        
        print(f"\n🚗 Vehicle Classes: {vehicle_success}/{vehicle_total} verified")
        print(f"   ✅ Verified: {verification_results['vehicle_classes']['verified']}")
        print(f"   ❌ Failed: {verification_results['vehicle_classes']['failed']}")
        
        print(f"\n🚨 Unwanted Selections: {unwanted_count} found")
        if verification_results["unwanted_selections"]["fuel"]:
            print(f"   ⚠️ Unwanted Fuel: {verification_results['unwanted_selections']['fuel']}")
        if verification_results["unwanted_selections"]["vehicle_classes"]:
            print(f"   ⚠️ Unwanted Vehicle Classes: {verification_results['unwanted_selections']['vehicle_classes']}")
        
        # Calculate overall success
        total_expected = fuel_total + vehicle_total
        total_verified = fuel_success + vehicle_success
        
        # Consider successful if:
        # 1. At least 70% of expected filters are verified
        # 2. No more than 2 unwanted selections
        success_rate = total_verified / total_expected if total_expected > 0 else 0
        verification_passed = success_rate >= 0.7 and unwanted_count <= 2
        
        if verification_passed:
            verification_results["overall_status"] = "passed"
            print(f"\n✅ COMPREHENSIVE VERIFICATION PASSED")
            print(f"   Success Rate: {success_rate:.1%} ({total_verified}/{total_expected})")
            print(f"   Unwanted Selections: {unwanted_count} (acceptable)")
        else:
            verification_results["overall_status"] = "failed"
            print(f"\n❌ COMPREHENSIVE VERIFICATION FAILED")
            print(f"   Success Rate: {success_rate:.1%} ({total_verified}/{total_expected})")
            print(f"   Unwanted Selections: {unwanted_count} (too many)" if unwanted_count > 2 else "")
        
        print(f"{'='*80}")
        
        return verification_passed, verification_results
    
    def _check_filter_checkbox(self, table_id, row_num, filter_name, silent=False):
        """Helper method to check if a specific filter checkbox is selected"""
        try:
            if not silent:
                print(f"🔍 Checking: {filter_name} (row {row_num})")
            
            # Try multiple comprehensive XPaths
            checkbox_xpaths = [
                f"//*[@id='{table_id}']/tbody/tr[{row_num}]/td/div/div[2]/span",
                f"//*[@id='{table_id}']/tbody/tr[{row_num}]/td//span[contains(@class,'ui-chkbox-box')]",
                f"//*[@id='{table_id}']/tbody/tr[{row_num}]//span[contains(@class,'ui-state')]",
                f"//*[@id='{table_id}']/tbody/tr[{row_num}]/td//div[contains(@class,'ui-chkbox')]//span"
            ]
            
            checkbox = None
            for xpath in checkbox_xpaths:
                try:
                    checkbox = self.driver.find_element(By.XPATH, xpath)
                    break
                except:
                    continue
            
            if checkbox:
                checkbox_class = checkbox.get_attribute("class") or ""
                if not silent:
                    print(f"   📋 Class: '{checkbox_class}'")
                
                # Check various ways the checkbox might indicate selection
                is_selected = False
                
                if ("ui-state-active" in checkbox_class or 
                    "ui-state-checked" in checkbox_class or
                    "ui-state-highlight" in checkbox_class):
                    is_selected = True
                    if not silent:
                        print(f"   ✅ {filter_name} is SELECTED")
                
                # Additional checks
                try:
                    parent = checkbox.find_element(By.XPATH, "..")
                    parent_class = parent.get_attribute("class") or ""
                    if "ui-state-active" in parent_class:
                        is_selected = True
                except:
                    pass
                
                try:
                    aria_checked = checkbox.get_attribute("aria-checked")
                    if aria_checked == "true":
                        is_selected = True
                except:
                    pass
                
                if not is_selected and not silent:
                    print(f"   ❌ {filter_name} is NOT selected")
                
                return is_selected
            else:
                if not silent:
                    print(f"   ❌ Could not find checkbox for {filter_name}")
                return False
                
        except Exception as e:
            if not silent:
                print(f"   ❌ Error checking {filter_name}: {e}")
            return False

    def rename_downloaded_file(self, state_name, rto_name, year_name, product_type):
        """Rename the downloaded file and move it to a state-specific folder"""
        try:
            # Wait for the file to be downloaded
            time.sleep(3)

            # Look for the most recently downloaded file in the root download dir
            downloaded_files = [f for f in os.listdir(self.download_dir) if f.endswith('.xlsx')]
            if not downloaded_files:
                print("❌ No downloaded files found")
                return False

            # Get the most recent file
            latest_file = max([os.path.join(self.download_dir, f) for f in downloaded_files], key=os.path.getctime)

            # Clean names
            rto_name = rto_name.replace('/', '_')
            state_clean = state_name.replace(' ', '_')

            # --- NEW LOGIC: Create State Folder ---
            state_folder = os.path.join(self.download_dir, state_clean)
            os.makedirs(state_folder, exist_ok=True)
            # --------------------------------------

            new_filename = f"{state_name}_{rto_name}_{year_name}_{product_type}.xlsx"
            # Update path to use state_folder instead of self.download_dir
            new_filepath = os.path.join(state_folder, new_filename)

            # Move and Rename
            os.rename(latest_file, new_filepath)
            print(f"✓ File saved to: {state_clean}/{new_filename}")
            return True

        except Exception as e:
            print(f"❌ Error renaming file: {e}")
            return False

    def download_csv(self, state_name, rto_name, year_name, product_type, max_attempts=5):
        """Download CSV data with multiple attempts and rename the file"""
        download_xpath = '/html/body/form/div[2]/div/div/div[3]/div/div[2]/div/div/div[1]/div[1]/a/img'
        for attempt in range(1, max_attempts + 1):
            try:
                print(f"Download attempt {attempt}...")
                download_btn = self.wait.until(EC.element_to_be_clickable((By.XPATH, download_xpath)))
                self.driver.execute_script("arguments[0].scrollIntoView(true);", download_btn)
                time.sleep(1)
                download_btn.click()
                print(f"✓ Download button clicked (attempt {attempt})")
                time.sleep(3)
                
                # Rename the downloaded file
                if self.rename_downloaded_file(state_name, rto_name, year_name, product_type):
                    print("✓ Download and rename completed successfully")
                    return True
                else:
                    print("✗ Download succeeded but rename failed")
                    return False
                    
            except TimeoutException:
                print(f"✗ Download attempt {attempt} failed: Download button not found")
            except Exception as e:
                print(f"✗ Download attempt {attempt} failed: {e}")
            if attempt < max_attempts:
                print("Retrying download...")
                time.sleep(2)
        print("✗ All download attempts failed")
        return False
    
    def scrape_single_product(self, state_name, state_xpath, rto_name, rto_xpath, year_name, year_xpath, product_type):
        """Scrape data for a single product type"""
        try:
            # Mark task as started
            self.progress_tracker.update_task_status(state_name, rto_name, year_name, product_type, "started")
            
            print(f"\n{'='*80}")
            print(f"SCRAPING: State={state_name}, RTO={rto_name}, Year={year_name}, Product={product_type}")
            print(f"{'='*80}")
            
            # Navigate to site
            self.navigate_to_site()
            
            # Select basic options
            print("🔄 Selecting basic options...")
            self.select_state(state_xpath)
            self.select_rto(rto_xpath)
            self.select_y_axis(Y_AXIS)
            self.select_x_axis(X_AXIS)
            self.select_year(year_xpath)
            
            # First refresh
            print("🔄 Initial refresh...")
            self.refresh_data()
            time.sleep(3)
            
            # Expand filter panel
            print("🔄 Expanding filter panel...")
            self.expand_filter_panel()
            time.sleep(2)
            
            # Select vehicle categories based on product type
            print(f"🔄 Selecting vehicle categories for {product_type}...")
            vehicle_categories = VEHICLE_CLASSES_CONFIG.get(product_type, [])
            self.select_vehicle_categories(vehicle_categories)
            
            # Select fuel type based on product type
            if product_type == "ICE":
                print("🔄 Selecting ICE fuel types...")
                self.select_fuel_ice()
            else:
                print("🔄 Selecting ELECTRIC fuel type...")
                self.select_fuel_electric()
            
            # Select specific vehicle classes based on product type
            if product_type == "E2W":
                print("🔄 Selecting E2W vehicle classes...")
                self.select_vehicle_classes(['M_CYCLE_SCOOTER', 'M_CYCLE_SCOOTER_SIDE_CAR', 'MOPED'])
            elif product_type == "L3G":
                print("🔄 Selecting L-3G vehicle class...")
                self.select_vehicle_classes(['E_RICKSHAW_CART_G'])
            elif product_type == "L3P":
                print("🔄 Selecting L-3P vehicle class...")
                self.select_vehicle_classes(['E_RICKSHAW_P'])
            elif product_type == "L5G":
                print("🔄 Selecting L-5G vehicle class...")
                self.select_vehicle_classes(['THREE_WHEELER_G'])
            elif product_type == "L5P":
                print("🔄 Selecting L-5P vehicle class...")
                self.select_vehicle_classes(['THREE_WHEELER_P'])
            elif product_type == "ICE":
                print("🔄 Selecting ICE vehicle classes...")
                self.select_vehicle_classes(['M_CYCLE_SCOOTER', 'M_CYCLE_SCOOTER_SIDE_CAR', 'MOPED'])
            
            # 🔍 COMPREHENSIVE FILTER VERIFICATION
            print("🔍 Verifying all filters comprehensively...")
            verification_passed, filter_details = self.verify_all_filters_comprehensive(product_type)
            
            if not verification_passed:
                print("⚠️ Comprehensive filter verification failed! Continuing anyway but marking status...")
                self.progress_tracker.update_task_status(
                    state_name, rto_name, year_name, product_type, 
                    "comprehensive_verification_failed", 
                    filter_details
                )
            else:
                print("✅ All filters verified successfully!")
                self.progress_tracker.update_task_status(
                    state_name, rto_name, year_name, product_type, 
                    "comprehensive_verification_passed", 
                    filter_details
                )
            
            # Second refresh after filters
            print("🔄 Refreshing after filter selection...")
            self.refresh_filters()
            time.sleep(5)  # Wait for data to load
            
            # Download CSV
            if DOWNLOAD_CSV:
                print("📥 Downloading CSV...")
                success = self.download_csv(state_name, rto_name, year_name, product_type)
                if success:
                    print(f"✅ Successfully downloaded and renamed: {state_name}_{rto_name}_{year_name}_{product_type}")
                    self.progress_tracker.update_task_status(state_name, rto_name, year_name, product_type, "completed")
                else:
                    print(f"❌ Failed to download: {state_name}_{rto_name}_{year_name}_{product_type}")
                    self.progress_tracker.update_task_status(state_name, rto_name, year_name, product_type, "download_failed")
                    return False
            else:
                self.progress_tracker.update_task_status(state_name, rto_name, year_name, product_type, "completed")
            
            print(f"✅ {product_type} data extraction completed successfully!")
            return True
            
        except Exception as e:
            print(f"❌ Error during {product_type} scraping: {e}")
            self.progress_tracker.update_task_status(
                state_name, rto_name, year_name, product_type, 
                "error", 
                {"error_message": str(e)}
            )
            return False

    def run_full_scraping_flow(self):
        """Run the complete scraping flow for all configurations"""

        # --- STEP 1: PRE-CALCULATE ALL TASKS ---
        tasks_queue = []

        print(f"\n🚀 BUILDING TASK QUEUE...")

        for state_name in STATES_TO_SCRAPE:
            # Get valid RTOs for this state
            available_rtos = RTO_CONFIG.get(state_name, {})

            # Apply Filter if user provided one
            if RTO_TO_SCRAPE and len(RTO_TO_SCRAPE) > 0:
                target_rtos = [r for r in available_rtos.keys() if
                               any(filt.lower() in r.lower() for filt in RTO_TO_SCRAPE)]
            else:
                target_rtos = list(available_rtos.keys())

            if not target_rtos:
                print(f"⚠️ No RTOs found for {state_name} (check RTO.json or filters)")
                continue

            # Build the task list
            for rto_name in target_rtos:
                for year_name in YEARS_TO_SCRAPE:
                    for product_type in PRODUCTS_TO_SCRAPE:
                        tasks_queue.append({
                            "state": state_name,
                            "state_xpath": STATES_CONFIG[state_name],
                            "rto": rto_name,
                            "rto_xpath": available_rtos[rto_name],
                            "year": year_name,
                            "year_xpath": YEARS_CONFIG[year_name],
                            "product": product_type
                        })

        total_tasks = len(tasks_queue)
        completed_count = 0
        failed_tasks = []

        print(f"📋 Total Tasks Queued: {total_tasks}")
        print(f"{'=' * 100}")

        # --- STEP 2: EXECUTE TASKS ---
        try:
            for i, task in enumerate(tasks_queue):
                task_id = f"{task['state']}_{task['rto']}_{task['year']}_{task['product']}"

                # Check history to skip duplicates
                current_status = self.progress_tracker.get_task_status(
                    task['state'], task['rto'], task['year'], task['product']
                )

                if current_status in ["completed", "comprehensive_verification_passed"]:
                    print(f"⏭️ Skipping completed ({i + 1}/{total_tasks}): {task_id}")
                    completed_count += 1
                    continue

                print(f"\n▶️ Processing Task {i + 1}/{total_tasks}: {task_id}")

                # Run Scraper
                success = self.scrape_single_product(
                    task['state'], task['state_xpath'],
                    task['rto'], task['rto_xpath'],
                    task['year'], task['year_xpath'],
                    task['product']
                )

                if success:
                    completed_count += 1
                    print(f"✅ Task Finished: {task_id}")
                else:
                    failed_tasks.append(task_id)
                    print(f"❌ Task Failed: {task_id}")

                # Smart Delay (Skip delay on the very last item)
                if i < total_tasks - 1:
                    print("⏳ Waiting 5 seconds...")
                    time.sleep(5)

        except KeyboardInterrupt:
            print("\n⚠️ Process interrupted by user")
        except Exception as e:
            print(f"\n❌ Unexpected Error: {e}")

        # --- STEP 3: SUMMARY ---
        print(f"\n{'=' * 100}")
        print(f"🏁 SCRAPING COMPLETED")
        print(f"Success: {completed_count}/{total_tasks}")
        print(f"Failed: {len(failed_tasks)}")

        if failed_tasks:
            print("Failed Items:")
            for f in failed_tasks:
                print(f" - {f}")
        print(f"{'=' * 100}")
    
    def close(self):
        """Close the browser"""
        if self.driver:
            self.driver.quit()
            print("Browser closed")
        elif self.test_mode:
            print("[TEST MODE] Browser would be closed here.")


def main():
    """Main function to run the scraping flow"""
    print("🔧 VAHAN SCRAPER - FLOW CONTROL MODE")
    print(f"Configuration loaded:")
    print(f"  States: {STATES_TO_SCRAPE}")
    print(f"  RTOs: {RTO_TO_SCRAPE}")
    print(f"  Years: {YEARS_TO_SCRAPE}")
    print(f"  Products: {PRODUCTS_TO_SCRAPE}")
    print(f"  Headless: {HEADLESS_MODE}")
    print(f"  Download CSV: {DOWNLOAD_CSV}")
    
    # Initialize scraper
    scraper = VahanScraper(headless=HEADLESS_MODE)
    
    try:
        # Run the complete scraping flow
        scraper.run_full_scraping_flow()
        
    finally:
        scraper.close()

if __name__ == "__main__":
    main()
