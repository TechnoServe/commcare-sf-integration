def map_status(value, mapping_dict, default="Undefined"):
    return mapping_dict.get(value, default)


EXPORTING_STATUS_MAP = {
    "1": "Exporter",
    "2": "Non exporter",
}

MANAGER_ROLE_MAP = {
    "1": "Wet mill manager"
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
    "4": "Pinhalense"
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

# Mappings for Manager Needs Assessment survey
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


# Training Attendance Recording mappings
TRAINING_TOPIC_MAP = {
    "1": "Machine Operation & Maintenance",
    "2": "Coffee Processing & Quality Control",
    "3": "Sustainability Standards Overview",
    "4": "Social Responsibility & Ethics",
    "5": "Occupation Health and Security",
    "6": "Environmental Responsibility",
    "7": "Gender Inclusion",
    "8": "Bookkeeping & Financial Management"
}
TRAINING_STATUS_MAP = {
    "1": "New",
    "2": "Refresher"
}

# Transformation for Waste Water Management survey
# Map only 'material' code to label and any lagoon photo fields

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


def transform_waste_water_management(survey_data, domain=None, instance_id=None):

    transformed = survey_data.copy()
    # 1. Lagoons mapping
    lagoons = transformed.get('lagoons')
    if isinstance(lagoons, dict):
        mat = lagoons.get('material')
        if isinstance(mat, str) and mat in LAGOON_MATERIAL_MAP:
            lagoons['material'] = LAGOON_MATERIAL_MAP[mat]

        pic = lagoons.get('photo')
        if pic:
            lagoons['photo'] = f'https://www.commcarehq.org/a/tns-proof-of-concept/api/form/attachment/8ade6744-f875-4654-8abb-834cdfbb6aed/{pic}'

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
            vet['photo'] = f'https://www.commcarehq.org/a/tns-proof-of-concept/api/form/attachment/8ade6744-f875-4654-8abb-834cdfbb6aed/{pic}'

        transformed['vetiver_wetland'] = vet
    return transformed

def transform_training_attendance_recording(survey_data):
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
    pic = transformed.get('picture_of_trainees_group')
    if pic:
        transformed['picture_of_trainees_group'] = transformed['photo_of_cherry_receipts'] = f'https://www.commcarehq.org/a/tns-proof-of-concept/api/form/attachment/8ade6744-f875-4654-8abb-834cdfbb6aed/{pic}'
    return transformed

def transform_manager_needs_assessment(survey_data):
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
    if isinstance(pom, dict) and 'coffee_station_issues' in pom:
        del pom['coffee_station_issues']
        transformed['perspective_of_manager'] = pom

    return transformed

def transform_kpis(survey_data):
    """
    Transformation for KPIs survey:
    - Converts 'photo_of_cherry_receipts' filename into full URL using domain and instanceID.
    """
    transformed = survey_data.copy()
    pic = transformed.get('photo_of_cherry_receipts')
    if pic:
        transformed['photo_of_cherry_receipts'] = f'https://www.commcarehq.org/a/tns-proof-of-concept/api/form/attachment/8ade6744-f875-4654-8abb-834cdfbb6aed/{pic}'
        
    return transformed

def transform_infrastructure(survey_data):
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
    # which_of_the_following_needs_repair
    rep = transformed.get('which_of_the_following_needs_repair')
    if isinstance(rep, str):
        transformed['which_of_the_following_needs_repair'] = [
            INFRASTRUCTURE_REPAIR_MAP[c] for c in rep.split() if c in INFRASTRUCTURE_REPAIR_MAP
        ]
    # are_the_following_in_good_state_of_repair
    good = transformed.get('are_the_following_in_good_state_of_repair')
    if isinstance(good, str):
        transformed['are_the_following_in_good_state_of_repair'] = [
            INFRASTRUCTURE_REPAIR_MAP[c] for c in good.split() if c in INFRASTRUCTURE_REPAIR_MAP
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

def transform_cpqi(survey_data):
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

def transform_financials(survey_data):
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

def transform_employees(survey_data):
    """
    Specific transformation for Employees survey:
    - Maps 'accountant' and 'sustainability_officer' from "1"/"0" to "yes"/"no"
    - Leaves all other fields as numeric values (floats).
    """
    transformed = {}
    for key, val in survey_data.items():
        if key in ('accountant', 'sustainability_officer') and val in ('1', '0'):
            transformed[key] = 'yes' if val == '1' else 'no'
        else:
            try:
                transformed[key] = float(val)
            except:
                transformed[key] = val
    return transformed

# Map survey names to their transformation functions
SURVEY_TRANSFORMATIONS = {
    "cpqi": transform_cpqi,
    "employees": transform_employees,
    "financials": transform_financials,
    "infrastructure": transform_infrastructure,
    "kpis": transform_kpis,
    "manager_needs_assessment": transform_manager_needs_assessment,
    "training_attendance_recording": transform_training_attendance_recording,
    "waste_water_management": transform_waste_water_management
}