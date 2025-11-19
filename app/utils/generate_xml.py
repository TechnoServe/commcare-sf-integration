from datetime import datetime
import html

def safe_escape(value):
    return html.escape(str(value)).replace('/', '&#x2F;') if value else ""

def safe_int(value):
    """
    Safely converts a value to an integer or float, returning its string representation.
    If the value is not a valid integer or float, returns an empty string.
    """
    if value is None or not str(value).strip():  # Handle None or empty/whitespace strings
        return ""

    try:
        # Try to convert to int first
        return str(int(value))
    except ValueError:
        try:
            # If int conversion fails, try to convert to float
            return str(float(value))
        except ValueError:
            # If both conversions fail, return an empty string
            return ""

def generate_xml(job_name, job_id, data, project_unique_id):
    
    if job_name == 'Participant':
        first_time_stamp = datetime.now()
        
        # Extract and format participant names
        firstname = data.get("participantName", "")
        middlename = data.get("participantMiddleName", "")
        lastname = data.get("participantLastName", "")
        fullname = f"{firstname} {middlename} {lastname}" if middlename else f"{firstname} {lastname}"

        # Determine farmer number based on primary household membership
        farmernumber = (
            "1" if data.get("participantPrimaryHouseholdMember") == "Yes" else 
            "2" if data.get("participantPrimaryHouseholdMember") == "No" else ""
            )

        # Determine specific IDs based on project
        coopno = data.get("participantOtherIDNumber", "") if project_unique_id in ["coffee_ke_2024ac", "coffee_ke_2024bc"] else ''
        national_id_number = data.get("participantOtherIDNumber", "") if project_unique_id in ["coffee_bdi_kahawatu_2024c", "coffee_bdi_tns_2024c"] else ''
        grower_number = data.get("participantOtherIDNumber", "") if project_unique_id in ["coffee_zw_2024c", "coffee_zw_c21_ag", "coffee_zim", "coffee_zim_c2_ag"] else ''

        
        # Determine Farm Size/Number of Coffee trees based on project
        farm_size = str(safe_int(data.get("householdFarmSize", "0"))) if project_unique_id in ["coffee_ke_2024ac", "coffee_ke_2024bc", "coffee_zw_2024c", "coffee_zw_c21_ag", "coffee_zim", "coffee_zim_c2_ag"] else data.get("householdFarmSize", "0")
        
        return f'''<?xml version="1.0" ?>
            <data xmlns:jrm="http://dev.commcarehq.org/jr/xforms" xmlns="http://openrosa.org/formdesigner/3E266629-AFD8-4A1C-8825-1DCDDF24E5A8" uiVersion="1" version="325" name="New Participant">
                <Name>{safe_escape(fullname)}</Name>
                <First_Name>{safe_escape(firstname)}</First_Name>
                <Middle_Name>{safe_escape(middlename)}</Middle_Name>
                <Last_Name>{safe_escape(lastname)}</Last_Name>
                <Age>{safe_int(data.get("participantAge", ""))}</Age>
                <Gender>{safe_escape(data.get("participantGender", ""))}</Gender>
                <Phone_Number>{safe_int(data.get("participantPhoneNumber", ""))}</Phone_Number>
                <Farmer_Id>{safe_escape(data.get("tnsId", ""))}</Farmer_Id>
                <Farmer_Number>{safe_int(farmernumber)}</Farmer_Number>
                <Cooperative_Membership_Number>{coopno}</Cooperative_Membership_Number>
                <Grower_Number>{grower_number}</Grower_Number>
                <National_ID_Number>{safe_escape(national_id_number)}</National_ID_Number>
                <Household_Id>{safe_escape(data.get("householdId", ""))}</Household_Id>
                <Household_PIMA_Id>{safe_escape(data.get("householdPIMAId", ""))}</Household_PIMA_Id>
                <Shade_Trees>{safe_int(data.get("shadeTrees", ""))}</Shade_Trees>
                <Household_Number>{safe_int(data.get("HHID", "")) if data.get("HHID", "") else ""}</Household_Number>
                <Number_of_Trees>{farm_size}</Number_of_Trees>
                <Parent_Id>{safe_escape(data.get("trainingGroupId", ""))}</Parent_Id>
                <Status>{safe_escape(data.get("status", ""))}</Status>
                <Primary_Household_Member>{safe_escape(data.get("participantPrimaryHouseholdMember", ""))}</Primary_Household_Member>
                <Case_Id>{safe_escape(data.get("commCareCaseId", ""))}</Case_Id>
                <Name_Household_Concat>{safe_escape(fullname)} {safe_int(data.get("HHID", ""))}-{safe_int(farmernumber)}</Name_Household_Concat>
                <n0:case case_id="{safe_escape(data.get('commCareCaseId', ''))}" date_modified="{safe_escape(datetime.now())}" user_id="e926526fc13b126fffdb6d001f25b269" xmlns:n0="http://commcarehq.org/case/transaction/v2">
                    <n0:create>
                        <n0:case_name>{safe_escape(fullname)}</n0:case_name>
                        <n0:owner_id>{safe_escape(data.get("ccMobileWorkerGroupId", ""))}</n0:owner_id>
                        <n0:case_type>{safe_escape(project_unique_id)}_participant</n0:case_type>
                    </n0:create>
                    <n0:update>
                        <n0:Case_Id>{safe_escape(data.get("commCareCaseId", ""))}</n0:Case_Id>
                        <n0:First_Name>{safe_escape(firstname)}</n0:First_Name>
                        <n0:Middle_Name>{safe_escape(middlename)}</n0:Middle_Name>
                        <n0:Last_Name>{safe_escape(lastname)}</n0:Last_Name>
                        <n0:Age>{safe_int(data.get("participantAge", ""))}</n0:Age>
                        <n0:Gender>{safe_escape(data.get("participantGender", ""))}</n0:Gender>
                        <n0:Phone_Number>{safe_int(data.get("participantPhoneNumber", ""))}</n0:Phone_Number>
                        <n0:Farmer_Id>{safe_escape(data.get("tnsId", ""))}</n0:Farmer_Id>
                        <n0:Farmer_Number>{safe_int(farmernumber)}</n0:Farmer_Number>
                        <n0:Cooperative_Membership_Number>{coopno}</n0:Cooperative_Membership_Number>
                        <n0:Grower_Number>{grower_number}</n0:Grower_Number>
                        <n0:National_ID_Number>{safe_escape(national_id_number)}</n0:National_ID_Number>
                        <n0:Household_Id>{safe_escape(data.get("householdId", ""))}</n0:Household_Id>
                        <n0:Household_PIMA_Id>{safe_escape(data.get("householdPIMAId", ""))}</n0:Household_PIMA_Id>
                        <n0:Shade_Trees>{safe_int(data.get("shadeTrees", ""))}</n0:Shade_Trees>
                        <n0:Household_Number>{safe_int(data.get("HHID", ""))}</n0:Household_Number>
                        <n0:Number_of_Trees>{farm_size}</n0:Number_of_Trees>
                        <n0:Status>{safe_escape(data.get("status", ""))}</n0:Status>
                        <n0:Primary_Household_Member>{safe_escape(data.get("participantPrimaryHouseholdMember", ""))}</n0:Primary_Household_Member>
                        <n0:Name_Household_Concat>{safe_escape(fullname)} {safe_escape(data.get("HHID", ""))}-{safe_escape(farmernumber)}</n0:Name_Household_Concat>
                        <n0:TNS_Id>{safe_escape(data.get("tnsId", ""))}</n0:TNS_Id>
                        <n0:Parent_Id>{safe_escape(data.get("trainingGroupId", ""))}</n0:Parent_Id>
                    </n0:update>
                    <n0:index>
                        <n0:parent case_type="{safe_escape(project_unique_id)}_training_group">{safe_escape(data.get('trainingGroupId', ''))}</n0:parent>
                    </n0:index>
                </n0:case>
                <n1:meta xmlns:n1="http://openrosa.org/jr/xforms">
                    <n1:deviceID>ID-10_t</n1:deviceID>
                    <n1:timeStart>{safe_escape(first_time_stamp)}</n1:timeStart>
                    <n1:timeEnd>{safe_escape(datetime.now())}</n1:timeEnd>
                    <n1:username>api</n1:username>
                    <n1:userID>e926526fc13b126fffdb6d001f25b269</n1:userID>
                    <n1:jobID>{job_id}</n1:jobID>
                </n1:meta>
            </data>
            '''
        
    elif job_name == 'Project Role':
        first_time_stamp = datetime.now()
        
        return f'''<?xml version="1.0" ?>
            <data xmlns="http://openrosa.org/formdesigner/CBD34B15-1442-4548-9B2D-C9937E3CB347" xmlns:jrm="http://dev.commcarehq.org/jr/xforms" name="New Project Role" uiVersion="1" version="1">
                <Name>{safe_escape(data.get("staffName", ""))}</Name>
                <TNS_Id>{safe_escape(data.get("tnsId", ""))}</TNS_Id>
                <City>{safe_escape(data.get("locationName", ""))}</City>
                <Role>{safe_escape(data.get("roleForCommCare", ""))}</Role>
                <Case_Id>{safe_escape(data.get("commCareCaseId", ""))}</Case_Id>
                <Current_Module>{safe_int(data.get("currentModule", ""))}</Current_Module>
                <Current_Module_Name>{safe_escape(data.get("currentModuleName", ""))}</Current_Module_Name>
                <Previous_Module>{safe_int(data.get("previousModule", ""))}</Previous_Module>
                <Previous_Module_Name>{safe_escape(data.get("previousModuleName", ""))}</Previous_Module_Name>
                <FFGs_Observed/>
                <Name_id_concat>{safe_escape(data.get("staffName", ""))} {safe_escape(data.get("tnsId", ""))}</Name_id_concat>
                <Salesforce_Staff_Id>{safe_escape(data.get("staffId", ""))}</Salesforce_Staff_Id>
                <n0:case xmlns:n0="http://commcarehq.org/case/transaction/v2" case_id="{safe_escape(data.get('commCareCaseId', ''))}" date_modified="{safe_escape(datetime.now())}" user_id="e926526fc13b126fffdb6d001f25b269">
                    <n0:create>
                        <n0:case_name>{safe_escape(data.get("staffName", ""))}</n0:case_name>
                        <n0:owner_id>{safe_escape(data.get("ccMobileWorkerGroupId", ""))}</n0:owner_id>
                        <n0:case_type>{safe_escape(project_unique_id)}_staff</n0:case_type>
                    </n0:create>
                    <n0:update>
                        <n0:Case_Id>{safe_escape(data.get("commCareCaseId", ""))}</n0:Case_Id>
                        <n0:Name_Id_Concat>{safe_escape(data.get("staffName", ""))} {safe_escape(data.get("tnsId", ""))}</n0:Name_Id_Concat>
                        <n0:Role>{safe_escape(data.get("roleForCommCare", ""))}</n0:Role>
                        <n0:City>{safe_escape(data.get("locationName", ""))}</n0:City>
                        <n0:TNS_Id>{safe_escape(data.get("tnsId", ""))}</n0:TNS_Id>
                        <n0:Current_Module>{safe_int(data.get("currentModule", ""))}</n0:Current_Module>
                        <n0:Current_Module_Name>{safe_escape(data.get("currentModuleName", ""))}</n0:Current_Module_Name>
                        <n0:Previous_Module>{safe_int(data.get("previousModule", ""))}</n0:Previous_Module>
                        <n0:Previous_Module_Name>{safe_escape(data.get("previousModuleName", ""))}</n0:Previous_Module_Name>
                        <n0:FFGs_Observed/>
                        <n0:Salesforce_Staff_Id>{safe_escape(data.get("staffId", ""))}</n0:Salesforce_Staff_Id>
                    </n0:update>
                </n0:case>
                <n1:meta xmlns:n1="http://openrosa.org/jr/xforms">
                    <n1:deviceID>ID-10_t</n1:deviceID>
                    <n1:timeStart>{safe_escape(first_time_stamp)}</n1:timeStart>
                    <n1:timeEnd>{safe_escape(datetime.now())}</n1:timeEnd>
                    <n1:username>api</n1:username>
                    <n1:userID>e926526fc13b126fffdb6d001f25b269</n1:userID>
                    <n1:jobID>{job_id}</n1:jobID>
                </n1:meta>
            </data>
            '''
    
    elif job_name == 'Training Session':
        first_time_stamp = datetime.now()
        
        return f'''<?xml version="1.0" ?>
            <data xmlns="http://openrosa.org/formdesigner/3FA54AF1-A35E-4163-BDB0-5094F709753C" xmlns:jrm="http://dev.commcarehq.org/jr/xforms" name="New Training Session" uiVersion="1" version="148">
                <Session_1_Date/>
                <Session_2_Date/>
                <Session_Status>{data.get("sessionStatus", "")}</Session_Status>
                <Training_Group_Name>{safe_escape(data.get("trainingGroupName", ""))}</Training_Group_Name>
                <Secondary_Parent_Id>{safe_escape(data.get("trainingGroupResponsibleStaff", ""))}</Secondary_Parent_Id>
                <Module_Name>{safe_escape(data.get("trainingModuleName", ""))}</Module_Name>
                <Module_Number>{safe_int(data.get("trainingModuleNumber", ""))}</Module_Number>
                <Current_Previous_Name>({safe_escape(data.get("currentPrevious", ""))}) {safe_escape(data.get("trainingModuleName", ""))}</Current_Previous_Name>
                <Training_Session_Name>{safe_escape(data.get("trainingModuleNumber", ""))} {safe_escape(data.get("trainingModuleName", ""))}</Training_Session_Name>
                <Current_Previous>{data.get("currentPrevious", "")}</Current_Previous>
                <Case_Id>{safe_escape(data.get("sessionId", ""))}</Case_Id>
                <Parent_Id>{safe_escape(data.get("trainingGroupCommCareId", ""))}</Parent_Id>
                <subcase_0>
                    <n0:case xmlns:n0="http://commcarehq.org/case/transaction/v2" case_id="{safe_escape(data.get('sessionId', ''))}" date_modified="{safe_escape(datetime.now())}" user_id="e926526fc13b126fffdb6d001f25b269">
                        <n0:create>
                            <n0:case_name>{safe_escape(data.get("trainingModuleNumber", ""))} {safe_escape(data.get("trainingModuleName", ""))}</n0:case_name>
                            <n0:owner_id>{safe_escape(data.get("ccMobileWorkerGroupId", ""))}</n0:owner_id>
                            <n0:case_type>{safe_escape(project_unique_id)}_training_session</n0:case_type>
                        </n0:create>
                        <n0:update>
                            <n0:Case_Id>{safe_escape(data.get("sessionId", ""))}</n0:Case_Id>
                            <n0:Date>{safe_escape(datetime.now())}</n0:Date>
                            <n0:Module_Name>{safe_escape(data.get("trainingModuleName", ""))}</n0:Module_Name>
                            <n0:Module_Number>{safe_int(data.get("trainingModuleNumber", ""))}</n0:Module_Number>
                            <n0:Current_Previous>{data.get("currentPrevious", "")}</n0:Current_Previous>
                            <n0:Current_Previous_Name>({safe_escape(data.get("currentPrevious", ""))}) {safe_escape(data.get("trainingModuleName", ""))}</n0:Current_Previous_Name>
                            <n0:Parent_Id>{safe_escape(data.get("trainingGroupCommCareId", ""))}</n0:Parent_Id>
                            <n0:Session_1_Date/>
                            <n0:Session_2_Date/>
                            <n0:Session_Status>{data.get("sessionStatus", "")}</n0:Session_Status>
                            <n0:Training_Group_Name>{safe_escape(data.get("trainingGroupName", ""))}</n0:Training_Group_Name>
                            <n0:Secondary_Parent_Id>{safe_escape(data.get("trainingGroupResponsibleStaff", ""))}</n0:Secondary_Parent_Id>
                        </n0:update>
                        <n0:index>
                            <n0:parent case_type="{safe_escape(project_unique_id)}_training_group">{safe_escape(data.get("trainingGroupCommCareId", ""))}</n0:parent>
                        </n0:index>
                    </n0:case>
                </subcase_0>
                <n1:case xmlns:n1="http://commcarehq.org/case/transaction/v2" case_id="{safe_escape(data.get('sessionId', ''))}" date_modified="{safe_escape(datetime.now())}" user_id="e926526fc13b126fffdb6d001f25b269"/>
                <n2:meta xmlns:n2="http://openrosa.org/jr/xforms">
                    <n2:deviceID>ID-10_t</n2:deviceID>
                    <n2:timeStart>{safe_escape(first_time_stamp)}</n2:timeStart>
                    <n2:timeEnd>{safe_escape(datetime.now())}</n2:timeEnd>
                    <n2:username>api</n2:username>
                    <n2:userID>e926526fc13b126fffdb6d001f25b269</n2:userID>
                    <n2:jobID>{job_id}</n2:jobID>
                </n2:meta>
            </data>
            '''
    
    elif job_name == 'Training Group':
        first_time_stamp = datetime.now()
        
        return f'''<?xml version="1.0" ?>
            <data xmlns="http://openrosa.org/formdesigner/3FA54AF1-A35E-4163-BDB0-5094F709753C" xmlns:jrm="http://dev.commcarehq.org/jr/xforms" name="New Training Group" uiVersion="1" version="1">
                <Name>{safe_escape(data.get("trainingGroupName", ""))}</Name>
                <FFG_Number>{safe_escape(data.get("tnsId", ""))}</FFG_Number>
                <Location>{safe_escape(data.get("locationName", ""))}</Location>
                <Measurement_Group>{safe_escape(data.get("measurementGroup", ""))}</Measurement_Group>
                <Cooperative_ID>{safe_escape(data.get("cooperative", ""))}</Cooperative_ID>
                <Household_Counter>{safe_int(data.get("householdCounter", ""))}</Household_Counter>
                <Name_Id_Concat>{safe_escape(data.get("trainingGroupName", ""))} {safe_escape(data.get("tnsId", ""))}</Name_Id_Concat>
                <Parent_Id>{safe_escape(data.get("staffId", ""))}</Parent_Id>
                <n0:case xmlns:n0="http://commcarehq.org/case/transaction/v2" case_id="{safe_escape(data.get("commCareCaseId", ""))}" date_modified="{safe_escape(datetime.now())}" user_id="e926526fc13b126fffdb6d001f25b269">
                    <n0:create>
                        <n0:case_name>{safe_escape(data.get("trainingGroupName", ""))}</n0:case_name>
                        <n0:owner_id>{safe_escape(data.get("ccMobileWorkerGroupId", ""))}</n0:owner_id>
                        <n0:case_type>{safe_escape(project_unique_id)}_training_group</n0:case_type>
                    </n0:create>
                    <n0:update>
                        <n0:Location>{safe_escape(data.get("locationName", ""))}</n0:Location>
                        <n0:Market>{safe_escape(data.get("market", ""))}</n0:Market>
                        <n0:Household_Counter>{safe_int(data.get("householdCounter", ""))}</n0:Household_Counter>
                        <n0:Name_Id_Concat>{safe_escape(data.get("trainingGroupName", ""))} {safe_escape(data.get("tnsId", ""))}</n0:Name_Id_Concat>
                        <n0:Parent_Id>{safe_escape(data.get("staffId", ""))}</n0:Parent_Id>
                        <n0:FFG_Number>{safe_escape(data.get("tnsId", ""))}</n0:FFG_Number>
                        <n0:Measurement_Group>{safe_escape(data.get("measurementGroup", ""))}</n0:Measurement_Group>
                        <n0:Cooperative_ID>{data.get("cooperative", "")}</n0:Cooperative_ID>
                    </n0:update>
                    <n0:index>
                        <n0:parent case_type="{safe_escape(project_unique_id)}_staff">{safe_escape(data.get("staffId", ""))}</n0:parent>
                    </n0:index>
                </n0:case>
                <n1:case xmlns:n1="http://commcarehq.org/case/transaction/v2" case_id="{safe_escape(data.get('commCareCaseId', ''))}" date_modified="{safe_escape(datetime.now())}" user_id="e926526fc13b126fffdb6d001f25b269"/>
                <n2:meta xmlns:n2="http://openrosa.org/jr/xforms">
                    <n2:deviceID>ID-10_t</n2:deviceID>
                    <n2:timeStart>{safe_escape(first_time_stamp)}</n2:timeStart>
                    <n2:timeEnd>{safe_escape(datetime.now())}</n2:timeEnd>
                    <n2:username>api</n2:username>
                    <n2:userID>e926526fc13b126fffdb6d001f25b269</n2:userID>
                    <n2:jobID>{job_id}</n2:jobID>
                </n2:meta>
            </data>
            '''
    
    elif job_name == 'Household Sampling':
        first_time_stamp = datetime.now()
        
        return f'''<?xml version="1.0" ?>
            <data xmlns:jrm="http://dev.commcarehq.org/jr/xforms" xmlns="http://openrosa.org/formdesigner/3E266629-AFD8-4A1C-8825-1DCDDF24E5A8" uiVersion="1" version="325" name="New Household Sample">
                <Name>{safe_int(data.get("householdName", ""))}</Name>
                <Number_Of_Members>{safe_int(data.get("numberOfMembers", ""))}</Number_Of_Members>
                <TNS_Id>{safe_escape(data.get("tnsId", ""))}</TNS_Id>
                <FV_AA_Visited>{safe_escape(data.get("fvAAVisited", ""))}</FV_AA_Visited>
                <FV_AA_Sampled>{safe_escape(data.get("fvAASampled", ""))}</FV_AA_Sampled>
                <FV_AA_Current_Sampling_Round>{safe_int(data.get("fvAACurrentSamplingRound", ""))}</FV_AA_Current_Sampling_Round>
                <Parent_Id>{safe_escape(data.get("trainingGroupId", ""))}</Parent_Id>
                <Status>{safe_escape(data.get("householdStatus", ""))}</Status>
                <Case_Id>{safe_escape(data.get("commCareCaseId", ""))}</Case_Id>
                <Module_Name>{safe_escape(data.get("moduleName", ""))}</Module_Name>
                <Module_Number>{safe_int(data.get("moduleNumber", ""))}</Module_Number>
                <Household_Participants>{safe_escape(data.get("householdParticipants", ""))}</Household_Participants>
                <n0:case xmlns:n0="http://commcarehq.org/case/transaction/v2" case_id="{safe_escape(data.get('commCareCaseId', ''))}" date_modified="{safe_escape(datetime.now())}" user_id="e926526fc13b126fffdb6d001f25b269">
                    <n0:create>
                        <n0:case_name>{safe_escape(data.get("householdName", ""))}</n0:case_name>
                        <n0:owner_id>{safe_escape(data.get("ccMobileWorkerGroupId", ""))}</n0:owner_id>
                        <n0:case_type>{safe_escape(project_unique_id)}_household_samples</n0:case_type>
                    </n0:create>
                    <n0:update>
                        <n0:Case_Id>{safe_escape(data.get("commCareCaseId", ""))}</n0:Case_Id>
                        <n0:Name>{safe_int(data.get("householdName", ""))}</n0:Name>
                        <n0:Number_Of_Members>{safe_int(data.get("numberOfMembers", ""))}</n0:Number_Of_Members>
                        <n0:TNS_Id>{safe_escape(data.get("tnsId", ""))}</n0:TNS_Id>
                        <n0:FV_AA_Visited>{safe_escape(data.get("fvAAVisited", ""))}</n0:FV_AA_Visited>
                        <n0:FV_AA_Sampled>{safe_escape(data.get("fvAASampled", ""))}</n0:FV_AA_Sampled>
                        <n0:FV_AA_Current_Sampling_Round>{safe_int(data.get("fvAACurrentSamplingRound", ""))}</n0:FV_AA_Current_Sampling_Round>
                        <n0:Status>{safe_escape(data.get("householdStatus", ""))}</n0:Status>
                        <n0:Module_Name>{safe_escape(data.get("moduleName", ""))}</n0:Module_Name>
                        <n0:Module_Number>{safe_int(data.get("moduleNumber", ""))}</n0:Module_Number>
                        <n0:Household_Participants>{safe_escape(data.get("householdParticipants", ""))}</n0:Household_Participants>
                        <n0:Parent_Id>{safe_escape(data.get("trainingGroupId", ""))}</n0:Parent_Id>
                    </n0:update>
                    <n0:index>
                        <n0:parent case_type="{safe_escape(project_unique_id)}_training_group">{safe_escape(data.get("trainingGroupId", ""))}</n0:parent>
                    </n0:index>
                </n0:case>
                <n1:meta xmlns:n1="http://openrosa.org/jr/xforms">
                    <n1:deviceID>ID-10_t</n1:deviceID>
                    <n1:timeStart>{safe_escape(first_time_stamp)}</n1:timeStart>
                    <n1:timeEnd>{safe_escape(datetime.now())}</n1:timeEnd>
                    <n1:username>api</n1:username>
                    <n1:userID>e926526fc13b126fffdb6d001f25b269</n1:userID>
                    <n1:jobID>{job_id}</n1:jobID>
                </n1:meta>
            </data>
            '''