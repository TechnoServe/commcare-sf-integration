from datetime import datetime

def map_status(value, mapping_dict, default="Undefined"):
    return mapping_dict.get(value, default)


EXPORTING_STATUS_MAP = {
    "1": "Exporter",
    "2": "Non exporter",
}

MANAGER_ROLE_MAP = {
    "Wet Mill Registration - ET": {
        "1": "General manager",
        "2": "Site/factory manager"
    },
    "Wet Mill Registration - BU": {
        "1": "Wet mill manager"
    }
}

WET_MILL_STATUS_MAP = {
    "1": "Cooperative",
    "2": "Private"
}

INFRASTRUCTURE_WATER_SOURCE_MAP = {
    "1": "River",
    "2": "Dam",
    "3": "Water pan",
    "4": "Piped system (local municipality)",
    "5": "Borehole",
    "6": "Spring",
    "7": "Roof catchment (rainwater)"
}

# 1. Infrastructure Mapping 

# Mapping for infrastructure needs repair options (1–16, 0 = None)
INFRASTRUCTURE_REPAIR_MAP = {
    "1": "Constant, clean source of water",
    "2": "Water circulation and/or treatment",
    "3": "Water meter for measurement",
    "4": "Floatation tank",
    "5": "Cherry reception hopper",
    "6": "Fermentation tanks",
    "7": "Grading channels",
    "8": "Pulp hopper",
    "9": "General mills area clean and orderly",
    "10": "Drying tables in a good state of repair",
    "11": "Storage area clean",
    "12": "Weighing scale is calibrated",
    "13": "Pulp machine calibrated and oiled",
    "14": "Moisture meter, thermometer, hygrometer",
    "15": "Cherry purchasing receipts in stock",
    "16": "Covering materials available",
    "0": "None"
}

INFRASTRUCTURE_PULPING_BRAND_MAP = {
    "1": "Penagos",
    "2": "Mckinnon",
    "3": "Bendal",
    "4": "Pinhalense",
    "5": "Pre-agard",
    "6": "Agard"
}
INFRASTRUCTURE_PULPING_TYPE_MAP = {
    "1": "Disc",
    "2": "Drum",
    "3": "Scree"
}
INFRASTRUCTURE_NETWORK_COVERAGE_MAP = {
    "1": "2G",
    "2": "3G",
    "3": "4G"
}

# 2. Manager Needs Assessment Mapping
MANAGER_DOCS_MAP = {
    "1": "Registration license",
    "2": "Tax number",
    "3": "Production or operational license for current year",
    "4": "Export license/number",
    "0": "None"
}

COFFEE_SALE_PERIOD_MAP = {
    "1": "Before the season, in order to access working capital",
    "2": "Late in the harvest season, as coffee accumulates in the warehouse",
    "3": "After the harvest season"
}

PRIMARY_BUYER_SERVICES_MAP = {
    "1": "Working capital to purchase coffee",
    "2": "Agronomic support",
    "3": "Processing training/quality support",
    "0": "No additional services offered"
}

MANAGER_BANKING_MAP = {
    "1": "No significant challenges accessing loans",
    "2": "Lack of physical assets for bank collateral",
    "3": "High interest rates",
    "4": "Need for existing purchase order from coffee buyer",
    "5": "Poor performance of coffee washing station",
    "6": "Lack of financial statements and information needed for financial institutions"
}

TECHNOLOGY_INFO_MAP = {
    "1": "Farmer names (e.g., Excel list of farmers)",
    "2": "Coffee volumes delivered to the coffee washing station",
    "3": "Accounting (e.g., regular entries and reconciliation in Excel or other tool)",
    "4": "Traceability (e.g., tracking daily batches and coffee washing station operations)",
    "5": "Farmer payments (e.g., digital record of farmer payments)",
    "6": "Do not use any digital tool"
}

# 3. Training Attendance Mapping (Dropped Burundi topics)
TRAINING_TOPIC_MAP = {
    '1': 'Environmental Responsibility',
    '2': 'Social Responsibility and Ethics',
    '3': 'Gender Training',
    '4': 'Occupational Health and Safety',
    '5': 'Sustainability Standards Overview',
    '6': 'Finance and Bookkeeping',
    '7': 'Post-Harvest Coffee Processing and Quality Training',
    '8': 'TASQ Overview ',
    '9': 'Inclusive Training',
    '10': 'Gender Training',
    '11': 'Regenerative Agriculture',
    '12': 'Farm-level Traceability',
    '13': 'Cooperative Good Governance',
    '14': 'Bookkeeping',
    '15': 'Quality Control and Processing Overview'
}

TRAINING_STATUS_MAP = {
    "1": "New",
    "2": "Refresher"
}

# 4. Waste Water Management Mapping

# Lagoon material mapping
LAGOON_MATERIAL_MAP = {
    "1": "Earth",
    "2": "Concrete"
}
# Vetiver wetland mappings
VETIVER_TYPE_MAP = {
    "1": "Single wetland",
    "2": "Stepped wetland (multiple wetlands)"
}
VETIVER_MAINTENANCE_MAP = {
    "1": "Leveling/correction",
    "2": "Soil bund maintenance",
    "3": "Vetiver grass replacement",
    "4": "Weeding",
    "5": "Vetiver cutting",
    "6": "Connecting channel maintenance",
    "0": "None"
}

# Advice to wetmill
WASTE_WATER_ADVICE_TYPES = {
    "1": "Pulp separation advice", 
    "2": "Lagoon or pond maintenance or location advice", 
    "3": "Vetiver wetland maintenance advice"
}

# Pulp
WASTE_WATER_MANAGEMENT_METHODS = {
    "1": "Open lagoon or pond", 
    "2": "Vetiver Wetland", 
    "0": "No wastewater management, released onto land or into river"
}

# 5. Water and Energy Use
WATER_MANAGEMENT_PULP_SEPARATION = {
    "1": "Pulp hopper", 
    "2": "Re-circulation pump with skin tower", 
    "eco-pulper": "Eco-pulper"
}

WATER_USE_METHODS = {
    "1": "Water meter", 
    "2": "Dip stick and tank size", 
    "0": "No method used"
}

WATER_USE_EFFORTS = {
    "1": "Turning water taps off when not in use", 
    "2": "Recirculation pump", 
    "3": "Eco pulper",
    "4": "Repairing all leaks in tanks, pipes and gate valves", 
    "0": "No efforts being made to reduce water consumption"
}

ENERGY_USE_SOURCES = {
    "1": "Mains electricity", 
    "2": "Diesel generator", 
    "3": "Solar panels"
}

# 6. Routine Visits
PURPOSE_OF_VISIT = {
    "1": "Performance of last year (Q1 and Q2)",
    "2": "Process quality check",
    "3": "SWOT analysis (Q1 and Q2)",
    "4": "Gender action plan meeting",
    "5": "Perform annual audit",
    "6": "Discuss annual audit feedback",
    "7": "Review visit (prior to advice from previous visits)"
}

# 7. KPIs
FARMER_PAYMENT_METHOD = {
    "1": "Direct payment",
    "2": "Broker"
}

def update_photo_url(energy_use, field_name, base_url, form_data):
    photo = energy_use.get(field_name)
    energy_use[field_name] = f"{base_url}/{form_data.get('id')}/{photo}" if photo else None
    return energy_use

base_url = f'https://www.commcarehq.org/a/tns-proof-of-concept/api/form/attachment/'


def transform_water_and_energy_use(survey_data, url_string):
    transformed = survey_data.copy()

     # 1. water_usage
    water_usage = transformed.get('water_usage')
    if isinstance(water_usage, dict):

        methods = water_usage.get('what_method_is_used_to_measure_water_use')
        if isinstance(methods, str) and methods in WATER_USE_METHODS:
            water_usage['what_method_is_used_to_measure_water_use'] = WATER_USE_METHODS[methods]

        efforts = water_usage.get('are_there_any_efforts_going_on_to_reduce_water_consumption')
        if isinstance(efforts, str):
            water_usage['are_there_any_efforts_going_on_to_reduce_water_consumption'] = [
                WATER_USE_EFFORTS.get(code) # Get Multiple values e.g: 1 4 5 6
                for code in efforts.split() # Split by empty space
                if code in WATER_USE_EFFORTS # Map to text values
            ]

        # Add other efforts
        other_efforts = water_usage.get('please_specify_the_other_efforts_going_on_to_reduce_the_water_consumption')
        water_usage['are_there_any_efforts_going_on_to_reduce_water_consumption'].append(other_efforts)

        is_there_a_record_book = water_usage.get("is_there_a_record_book")
        water_usage['is_there_a_record_book'] = 'yes' if is_there_a_record_book == '1' else 'no'


        pic = water_usage.get('photo_fo_the_office_records')
        if pic:
            water_usage['photo_fo_the_office_records'] = f'{url_string}/{pic}'
        else:
            water_usage['photo_fo_the_office_records'] = None

        water_meter_photo = water_usage.get('photo_of_water_meter')
        if pic:
            water_usage['photo_of_water_meter'] = f'{url_string}/{water_meter_photo}'
        else:
            water_usage['photo_of_water_meter'] = None

        transformed['water_usage'] = water_usage

    # 2. energy_use
    energy_use = transformed.get('energy_use')
    if isinstance(energy_use, dict):
        energy_source = energy_use.get('which_energy_source_is_used_at_the_wet_mill')
        if isinstance(energy_source, str):
            energy_use['which_energy_source_is_used_at_the_wet_mill'] = [
                ENERGY_USE_SOURCES.get(code) # Get Multiple values e.g: 1 4 5 6
                for code in energy_source.split() # Split by empty space
                if code in ENERGY_USE_SOURCES # Map to text values
            ]

        energy_rb = energy_use.get("is_there_an_energy_record_book_to_track_energy")
        energy_use['is_there_an_energy_record_book_to_track_energy'] = 'yes' if energy_rb == '1' else 'no'

        photo_fields = [
            'photo_of_the_electric_meter',
            'photo_of_the_diesel_generator',
            'photo_of_the_solar_panels',
            'photo_of_energy_record_book'
        ]

        for field in photo_fields:
            energy_use = update_photo_url(energy_use, field, base_url)

        transformed['energy_use'] = energy_use

    return transformed

def transform_waste_water_management(survey_data, url_string):

    transformed = survey_data.copy()
    # 1. Lagoons mapping
    lagoons = transformed.get('lagoons')
    if isinstance(lagoons, dict):
        mat = lagoons.get('material')
        if isinstance(mat, str) and mat in LAGOON_MATERIAL_MAP:
            lagoons['material'] = LAGOON_MATERIAL_MAP[mat]

        pic = lagoons.get('photo')
        if pic:
            lagoons['photo'] = f'{url_string}/{pic}'

        transformed['lagoons'] = lagoons

    # 2. Vetiver wetland mapping
    vet = transformed.get('vetiver_wetland')
    if isinstance(vet, dict):
        # type of wetland
        tw = vet.get('type_of_wetland')
        if isinstance(tw, str) and tw in VETIVER_TYPE_MAP:
            vet['type_of_wetland'] = VETIVER_TYPE_MAP[tw]

        # maintenance done 
        maint = vet.get('maintenance_done')
        if isinstance(maint, str):
            vet['maintenance_done'] = [
                VETIVER_MAINTENANCE_MAP.get(code)
                for code in maint.split()
                if code in VETIVER_MAINTENANCE_MAP
            ]

        # photo of vetiver wetland
        pic = vet.get('photo')
        if pic:
            vet['photo'] = f'{url_string}/{pic}'
        else:
            vet['photo'] = None

        transformed['vetiver_wetland'] = vet
    
    # 3. Advise to wetmill
    advise = transformed.get('advice_to_wet_mill')
    if isinstance(advise, dict):
        advice_type = advise.get('advice_type')

        if isinstance(advice_type, str):
            advise['advice_type'] = [
                WASTE_WATER_ADVICE_TYPES.get(code) # Get Advice types string EG: 1 4 5 6
                for code in advice_type.split() # Split by empty space
                if code in WASTE_WATER_ADVICE_TYPES # Map to text values
            ]

        transformed['advice_to_wet_mill'] = advise

    # 4. Pulp
    pulp = transformed.get('pulp_separator')
    if isinstance(pulp, dict):
        ww_methods = pulp.get('waste_water_management_methods')

        if isinstance(ww_methods, str):
            pulp['waste_water_management_methods'] = [
                WASTE_WATER_MANAGEMENT_METHODS.get(code) 
                for code in ww_methods.split() # Split by empty space
                if code in WASTE_WATER_MANAGEMENT_METHODS # Map to text values
            ]
        
        pulp_separation = pulp.get('how_is_the_pulp_separated')

        if isinstance(ww_methods, str):
            pulp['how_is_the_pulp_separated'] = [
                WATER_MANAGEMENT_PULP_SEPARATION.get(code) 
                for code in pulp_separation.split() # Split by empty space
                if code in WATER_MANAGEMENT_PULP_SEPARATION # Map to text values
            ]

        transformed['pulp_separator'] = pulp

    return transformed

def transform_wetmill_training(survey_data, url_string):
    transformed = survey_data.copy()
    # 1. training topic
    topic = transformed.get('training_topic')
    if isinstance(topic, str) and topic in TRAINING_TOPIC_MAP:
        transformed['training_topic'] = TRAINING_TOPIC_MAP[topic]
    # 2. training status
    status = transformed.get('training_status')
    if isinstance(status, str) and status in TRAINING_STATUS_MAP:
        transformed['training_status'] = TRAINING_STATUS_MAP[status]
    
    # 3. picture URL
    pic_trainees = transformed.get('picture_of_trainees_group')
    if pic_trainees:
        transformed['picture_of_trainees_group'] = f'{url_string}/{pic_trainees}'
    else:
        transformed['picture_of_trainees_group'] = None
        
    # 4. picture of attendance form
    pic_attendance = transformed.get('picture_of_training_attendance_form')
    if pic_attendance:
        transformed['picture_of_training_attendance_form'] = f'{url_string}/{pic_attendance}'
    else:
        transformed['picture_of_training_attendance_form'] = None
    
    return transformed



def transform_manager_needs_assessment(survey_data, url_string):
    """
    Manager Needs Assessment survey:
    - Map business_and_operations subfields (documents, coffee_sale_period, primary_buyer
      services).
    - Map banking challenges.
    - Map technology information_captured .
    """
    transformed = survey_data.copy()

    # Business & Operations
    bo = transformed.get('business_and_operations')
    if isinstance(bo, dict):
        # Documents
        docs = bo.get('documents')
        if isinstance(docs, str):
            bo['documents'] = [
                MANAGER_DOCS_MAP.get(code)
                for code in docs.split()
                if MANAGER_DOCS_MAP.get(code)
            ]
        # Coffee sale period
        csp = bo.get('coffee_sale_period')
        if isinstance(csp, str) and csp in COFFEE_SALE_PERIOD_MAP:
            bo['coffee_sale_period'] = COFFEE_SALE_PERIOD_MAP[csp]
        # Primary buyer additional services
        pbas = bo.get('primary_buyer_additional_services_yn')
        if isinstance(pbas, str):
            bo['primary_buyer_additional_services_yn'] = [
                PRIMARY_BUYER_SERVICES_MAP.get(code)
                for code in pbas.split()
                if PRIMARY_BUYER_SERVICES_MAP.get(code)
            ]

        # NOTE: this is extra JSON level deep than normal questions, I am flattening it NOTE: Better solution can be used :)
        dist = bo.get('distribution_of_revenues')
        if isinstance(dist, dict):
            # bring nested keys up one level
            for dk, dv in dist.items():
                # skip label fields
                if dk.endswith('_label'):
                    continue
                bo[dk] = dv
            # remove the nested dict
            bo.pop('distribution_of_revenues', None)
        transformed['business_and_operations'] = bo

    # Banking
    banking = transformed.get('banking')
    if isinstance(banking, dict):
        chal = banking.get('significant_challenges_accessing_loans')
        if isinstance(chal, str) and chal in MANAGER_BANKING_MAP:
            banking['significant_challenges_accessing_loans'] = MANAGER_BANKING_MAP[chal]
        transformed['banking'] = banking

    # Technology
    tech = transformed.get('technology')
    if isinstance(tech, dict):
        info = tech.get('information_captured')
        if isinstance(info, str):
            tech['information_captured'] = [
                TECHNOLOGY_INFO_MAP.get(code)
                for code in info.split()
                if TECHNOLOGY_INFO_MAP.get(code)
            ]
        transformed['technology'] = tech

    # operations.biggest_problems
    ops = transformed.get('operations')
    if isinstance(ops, dict) and 'biggest_problems' in ops:
        del ops['biggest_problems']
        transformed['operations'] = ops

    # perspective_of_manager.coffee_station_issues
    pom = transformed.get('perspective_of_manager')
    
    # perspective_of_manager_extra
    pom_extra = transformed.get('perspective_of_manager_extra')
    
    # add this to perspective_of_manager
    pom.update(pom_extra)
    
    if isinstance(pom, dict) and 'coffee_station_issues' in pom:
        del pom['coffee_station_issues']
        transformed['perspective_of_manager'] = pom

    return transformed

def transform_kpis(survey_data, url_string):
    transformed = survey_data.copy()
    pic = transformed.get('photo_of_cherry_receipts')
    if pic:
        transformed['photo_of_cherry_receipts'] = f'{url_string}/{pic}'
        
    fpm = transformed.get('farmer_payment_method')
    if isinstance(fpm, str) and fpm in FARMER_PAYMENT_METHOD:
        transformed['farmer_payment_method'] = FARMER_PAYMENT_METHOD[fpm]
        
    return transformed

def transform_infrastructure(survey_data, url_string):
    """
    Transformation for Infrastructure survey:
    - Map 'main_water_source' numeric codes to descriptive text.
    - which_of_the_following_needs_repair AND are_the_following_in_good_state_of_repair are multiple questions
    """
    transformed = survey_data.copy()
    # main_water_source
    code = transformed.get('main_water_source')
    if isinstance(code, str) and code in INFRASTRUCTURE_WATER_SOURCE_MAP:
        transformed['main_water_source'] = INFRASTRUCTURE_WATER_SOURCE_MAP[code]
        
    # are_the_following_in_good_state_of_repair
    good = transformed.get('are_the_following_in_good_state_of_repair')
    if isinstance(good, str): # Changed the logic since CommCare logic was changed too. These are now ALL equipment whether they need repair or not
        transformed['are_the_following_in_good_state_of_repair'] = [
            INFRASTRUCTURE_REPAIR_MAP[c] for c in good.split() if c in INFRASTRUCTURE_REPAIR_MAP and c not in transformed.get('which_of_the_following_needs_repair_check_all_that_apply', "").split()
        ]
        
    # which_of_the_following_needs_repair
    rep = transformed.get('which_of_the_following_needs_repair_check_all_that_apply')
    if isinstance(rep, str):
        transformed['which_of_the_following_needs_repair'] = [
            INFRASTRUCTURE_REPAIR_MAP[c] for c in rep.split() if c in INFRASTRUCTURE_REPAIR_MAP
        ]
    # pulping_machine_brand
    brand = transformed.get('pulping_machine_brand')
    if isinstance(brand, str) and brand in INFRASTRUCTURE_PULPING_BRAND_MAP:
        transformed['pulping_machine_brand'] = INFRASTRUCTURE_PULPING_BRAND_MAP[brand]
    # pulping_machine_type
    ptype = transformed.get('pulping_machine_type')
    if isinstance(ptype, str) and ptype in INFRASTRUCTURE_PULPING_TYPE_MAP:
        transformed['pulping_machine_type'] = INFRASTRUCTURE_PULPING_TYPE_MAP[ptype]
    # network_coverage
    net = transformed.get('network_coverage')
    if isinstance(net, str) and net in INFRASTRUCTURE_NETWORK_COVERAGE_MAP:
        transformed['network_coverage'] = INFRASTRUCTURE_NETWORK_COVERAGE_MAP[net]
    
    return transformed

def transform_cpqi(survey_data, url_string):
    """
    For CPQI convert all values '1'/'0' to yes or no.
    """
    transformed = survey_data.copy()
    # Map cherry_reception fields
    cherry = transformed.get('cherry_reception')
    if isinstance(cherry, dict):
        for field in ['cherry_sorting', 'cherry_weighing_essentials', 'quality_cherry_delivery']:
            val = cherry.get(field)
            if val in ('1', '0'):
                cherry[field] = 'yes' if val == '1' else 'no'
    # Map pulping fields
    pulping = transformed.get('pulping')
    if isinstance(pulping, dict):
        for field in ['machine_calibration', 'machine_cleanliness', 'timely_cherry_pulping', 'water_source_cleanliness']:
            val = pulping.get(field)
            if val in ('1', '0'):
                pulping[field] = 'yes' if val == '1' else 'no'
    # Map drying fields
    drying = transformed.get('drying')
    if isinstance(drying, dict):
        for field in ['bean_moisture_measurement', 'covering_coffee', 'drying_table_bean_depth', 'drying_table_flatness', 'parchment_sorting']:
            val = drying.get(field)
            if val in ('1', '0'):
                drying[field] = 'yes' if val == '1' else 'no'
    # Map fermentation fields
    fermentation = transformed.get('fermentation')
    if isinstance(fermentation, dict):
        for field in ['fermentation_monitoring', 'fermentation_tank_cleanliness']:
            val = fermentation.get(field)
            if val in ('1', '0'):
                fermentation[field] = 'yes' if val == '1' else 'no'
    # Map storage fields
    storage = transformed.get('storage')
    if isinstance(storage, dict):
        for field in ['orderly_store_registry', 'store_cleanliness']:
            val = storage.get(field)
            if val in ('1', '0'):
                storage[field] = 'yes' if val == '1' else 'no'

    # Map washing fields
    washing = transformed.get('washing')
    if isinstance(washing, dict):
        for field in ['washing_channel_cleanliness', 'washing_monitoring']:
            val = washing.get(field)
            if val in ('1', '0'):
                washing[field] = 'yes' if val == '1' else 'no'

    return transformed

def transform_financials(survey_data, url_string):
    """
    - Remove the 'survey_6___financials' key.
    - Remove any keys ending with '_label'.
    - Leave all other fields and values unchanged.
    """
    def clean(d):
        result = {}
        for k, v in d.items():
            if k == 'survey_6___financials' or k.endswith('_label'):
                continue
            if isinstance(v, dict):
                nested = clean(v)
                if nested is not None:
                    result[k] = nested
            else:
                result[k] = v
        return result
    return clean(survey_data)

# Transformation for employees survey
def transform_employees(survey_data, url_string):
    """
    Specific transformation for Employees survey:
    - Maps 'accountant' and 'sustainability_officer' from "1"/"0" to "yes"/"no"
    - Leaves all other fields as numeric values (floats).
    """
    transformed = {}
    for key, val in survey_data.items():
        if key in ('accountant', 'sustainability_officer', 'community_manager', 'certification_officer', 'machine_operator') and val in ('1', '0'):
            transformed[key] = 'yes' if val == '1' else 'no'
        else:
            try:
                transformed[key] = float(val)
            except:
                transformed[key] = val
    return transformed

# Transformation of Routine visit
def transformation_routine_visit(survey_data, url_string):

    transformed = {}
    
    # 1. map the purpose of visit (Include other)
    
    if survey_data.get('purpose_of_visit') == '99':
        transformed['purpose_of_visit'] = f'Other: {survey_data.get('specify_the_purpose_of_visit')}'
    else:
        transformed['purpose_of_visit'] = PURPOSE_OF_VISIT.get(survey_data.get('purpose_of_visit'))
        
    # 2. Summary of activity
    transformed['summary_of_activity'] = survey_data.get('summary_of_activity') 
    
    # 3. Picture of activity
    pic = survey_data.get('picture_of_activity')
    if pic:
        transformed['picture_of_activity'] = f'{url_string}/{pic}'
    else:
        transformed['picture_of_activity'] = None
        
    transformed['general_feedback'] = survey_data.get('general_feedback')
    
    return transformed
        
        
# Transformation for Cherry Weekly Price
def transform_cherry_weekly_price(survey_data, url_string):
    transformed = {}
    
    transformed["date"] = survey_data.get("cherry_week")
    date = survey_data.get("cherry_week")
    if date:
        # Convert date string to datetime object
        date = datetime.strptime(date, "%Y-%m-%d")
        transformed["cherry_week"] =  str(date.isocalendar().week)
    
    else:
        transformed["cherry_week"] = None
    
    transformed["cherry_price"] = survey_data.get("cherry_price")
    transformed["general_feedback"] = survey_data.get("general_feedback")
    
    return transformed

# Map survey names to their transformation functions
SURVEY_TRANSFORMATIONS = {
    "cpqi": transform_cpqi,
    "employees": transform_employees,
    "financials": transform_financials,
    "infrastructure": transform_infrastructure,
    "kpis": transform_kpis,
    "manager_needs_assessment": transform_manager_needs_assessment,
    "wet_mill_training": transform_wetmill_training,
    "waste_water_management": transform_waste_water_management, 
    "water_and_energy_use": transform_water_and_energy_use,
    "routine_visit": transformation_routine_visit,
    "cherry_weekly_price": transform_cherry_weekly_price
}