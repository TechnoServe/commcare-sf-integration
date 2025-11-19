from utils.registration_util import (
    process_household, 
    process_participant
    )
from utils.farm_visit_util import (
    process_farm_visit, 
    process_best_practices, 
    process_best_practice_results_erosion_control, 
    process_best_practice_results_cbb, 
    process_best_practice_results_chemicals_and_fertilizers,
    process_best_practice_results_clr,
    process_best_practice_results_compost_and_manure, 
    process_best_practice_results_pesticide_use_pest_problems, 
    process_best_practice_results_pesticide_use_sprays, 
    process_best_practice_results_pruning, 
    process_best_practice_results_weeding,
    process_best_practice_results_kitchen_garden,
    update_household_fvaa
    )
from utils.fis_util import (
    process_farm,
    process_varieties,
    update_household_fis
)
from utils.participant_check_util import process_participant_check_farm_visit_aa
from utils.logging_config import logger
import os

async def send_to_salesforce(data: dict, sf_connection):
    
    request_id = data.get("id")
    
    '''
    
    PART 1: Process Household and Participant (PR Registration and FV AA)
    
    '''
    
    # Step 1: Process household - PR registration
    try:
        logger.info({
            "message": "Processing household",
            "request_id": request_id
        })
        process_household(data, sf_connection)
    except Exception as e:
        logger.error({
            "message": "Error processing household",
            "request_id": request_id,
            "error": str(e)
        })
        return False, str(e)

    # Step 2: Process participant - PR registration
    try:
        logger.info({
            "message": "Processing participant",
            "request_id": request_id
        })
        process_participant(data, sf_connection)
    except Exception as e:
        logger.error({
            "message": "Error processing participant",
            "request_id": request_id,
            "error": str(e)
        })
        return False, str(e)
    
    '''
    
    PART 2: Process Farm Visit, Best Practices, and Best Practice Results - (FV Full & FV AA)
    
    '''
    
    # Step 3: Process farm visit
    try:
        logger.info({
            "message": "Processing farm visit",
            "request_id": request_id
        })
        process_farm_visit(data, sf_connection)
    except Exception as e:
        logger.error({
            "message": "Error processing farm visit",
            "request_id": request_id,
            "error": str(e)
        })
        return False, str(e)
    
    # Step 4: Process best practices
    try:
        logger.info({
            "message": "Processing best practices",
            "request_id": request_id
        })
        process_best_practices(data, sf_connection)
    except Exception as e:
        logger.error({
            "message": "Error processing best practices",
            "request_id": request_id,
            "error": str(e)
        })
        return False, str(e)
    
    # Step 5: Process best practice results - erosion control
    try:
        logger.info({
            "message": "Processing best practice results - erosion control",
            "request_id": request_id
        })
        process_best_practice_results_erosion_control(data, sf_connection)
    except Exception as e:
        logger.error({
            "message": "Error processing best practice results - erosion control",
            "request_id": request_id,
            "error": str(e)
        })
        return False, str(e)
    
    # Step 6: Process best practice results - chemicals and fertilizers
    try:
        logger.info({
            "message": "Processing best practice results - chemicals and fertilizers",
            "request_id": request_id
        })
        process_best_practice_results_chemicals_and_fertilizers(data, sf_connection)
    except Exception as e:
        logger.error({
            "message": "Error processing best practice results - chemicals and fertilizers",
            "request_id": request_id,
            "error": str(e)
        })
        return False, str(e)
    
    # Step 7: Process best practice results - cbb
    try:
        logger.info({
            "message": "Processing best practice results - cbb",
            "request_id": request_id
        })
        process_best_practice_results_cbb(data, sf_connection)
    except Exception as e:
        logger.error({
            "message": "Error processing best practice results - cbb",
            "request_id": request_id,
            "error": str(e)
        })
        return False, str(e)
    
    # Step 8: Process best practice results - clr
    try:
        logger.info({
            "message": "Processing best practice results - clr",
            "request_id": request_id
        })
        process_best_practice_results_clr(data, sf_connection)
    except Exception as e:
        logger.error({
            "message": "Error processing best practice results - clr",
            "request_id": request_id,
            "error": str(e)
        })
        return False, str(e)
    
    # Step 9: Process best practice results - pruning
    try:
        logger.info({
            "message": "Processing best practice results - pruning",
            "request_id": request_id
        })
        process_best_practice_results_pruning(data, sf_connection)
    except Exception as e:
        logger.error({
            "message": "Error processing best practice results - pruning",
            "request_id": request_id,
            "error": str(e)
        })
        return False, str(e)
    
    # Step 10: Process best practice results - weeding
    try:
        logger.info({
            "message": "Processing best practice results - weeding",
            "request_id": request_id
        })
        process_best_practice_results_weeding(data, sf_connection)
    except Exception as e:
        logger.error({
            "message": "Error processing best practice results - weeding",
            "request_id": request_id,
            "error": str(e)
        })
        return False, str(e)
    
    # Step 11: Process best practice results - pest problems
    try:
        logger.info({
            "message": "Processing best practice results - pest problems",
            "request_id": request_id
        })
        process_best_practice_results_pesticide_use_pest_problems(data, sf_connection)
    except Exception as e:
        logger.error({
            "message": "Error processing best practice results - pest problems",
            "request_id": request_id,
            "error": str(e)
        })
        return False, str(e)
    
    # Step 12: Process best practice results - pest sprays
    try:
        logger.info({
            "message": "Processing best practice results - pest sprays",
            "request_id": request_id
        })
        process_best_practice_results_pesticide_use_sprays(data, sf_connection)
    except Exception as e:
        logger.error({
            "message": "Error processing best practice results - pest sprays",
            "request_id": request_id,
            "error": str(e)
        })
        return False, str(e)
    
    # Step 13: Process best practice results - compost
    try:
        logger.info({
            "message": "Processing best practice results - compost",
            "request_id": request_id
        })
        process_best_practice_results_compost_and_manure(data, sf_connection)
    except Exception as e:
        logger.error({
            "message": "Error processing best practice results - compost",
            "request_id": request_id,
            "error": str(e)
        })
        return False, str(e)
    
    # Step 14: Process best practice results - kitchen garden
    try:
        logger.info({
            "message": "Processing best practice results - kitchen garden",
            "request_id": request_id
        })
        process_best_practice_results_kitchen_garden(data, sf_connection)
    except Exception as e:
        logger.error({
            "message": "Error processing best practice results - kitchen garden",
            "request_id": request_id,
            "error": str(e)
        })
        return False, str(e)
    
    # Step 15: Update household with latest visit record
    try:
        logger.info({
            "message": "Updating household data - FV",
            "request_id": request_id
        })
        update_household_fvaa(data, sf_connection)
    except Exception as e:
        logger.error({
            "message": "Error updating household data - FV",
            "request_id": request_id,
            "error": str(e)
        })
        return False, str(e)
    
    '''
    
    PART 3: Process Farm/Field/Plot data (FIS Zimbabwe)
    
    '''
    
    # Step 16: Process farm data - FIS Zimbabwe
    try:
        logger.info({
            "message": "Processing plot data - FIS",
            "request_id": request_id
        })
        process_farm(data, sf_connection)
    except Exception as e:
        logger.error({
            "message": "Error processing plot data - FIS",
            "request_id": request_id,
            "error": str(e)
        })
        return False, str(e)
    
    # Step 17: Process coffee variety data - FIS Zimbabwe
    try:
        logger.info({
            "message": "Processing coffee variety data - FIS",
            "request_id": request_id
        })
        process_varieties(data, sf_connection)
    except Exception as e:
        logger.error({
            "message": "Error processing coffee variety data - FIS",
            "request_id": request_id,
            "error": str(e)
        })
        return False, str(e)
    
    # Step 18: Update household - FIS Zimbabwe
    try:
        logger.info({
            "message": "Updating household data - FIS",
            "request_id": request_id
        })
        update_household_fis(data, sf_connection)
    except Exception as e:
        logger.error({
            "message": "Error updating household data - FIS",
            "request_id": request_id,
            "error": str(e)
        })
        return False, str(e)
    
    # Step 19: Upsert participant attendance check results - Farm Visit AA
    try:
        logger.info({
            "message": "Upserting participant attendance check results - Farm Visit AA",
            "request_id": request_id
        })
        process_participant_check_farm_visit_aa(data, sf_connection)
    except Exception as e:
        logger.error({
            "message": "Error upserting participant attendance check results - Farm Visit AA",
            "request_id": request_id,
            "error": str(e)
        })
        return False, str(e)
    
    
    return True, None