import datetime

def generate_xml(form_type, data, project_unique_id):
    
    if form_type == 'participant':
        first_time_stamp = datetime.datetime.now()
        case_type = f'{project_unique_id}_participant'
        
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

        
        return f'''<?xml version="1.0" ?>
            <data xmlns:jrm="http://dev.commcarehq.org/jr/xforms" xmlns="http://openrosa.org/formdesigner/3E266629-AFD8-4A1C-8825-1DCDDF24E5A8" uiVersion="1" version="325" name="New Participant">
            <Name>{fullname}</Name>
            <First_Name>{firstname}</First_Name>
            <Middle_Name>{middlename}</Middle_Name>
            <Last_Name>{lastname}</Last_Name>
            <Age>{data.get("participantAge", "")}</Age>
            <Gender>{data.get("participantGender", "")}</Gender>
            <Phone_Number>{data.get("participantPhoneNumber", "")}</Phone_Number>
            <Farmer_Id>{data.get("tnsId", "")}</Farmer_Id>
            <Farmer_Number>{farmernumber}</Farmer_Number>
            <Cooperative_Membership_Number>{coopno}</Cooperative_Membership_Number>
            <Grower_Number>{grower_number}</Grower_Number>
            <National_ID_Number>{national_id_number}</National_ID_Number>
            <Household_Id>{data.get("householdId", "")}</Household_Id>
            <Household_Number>{data.get("HHID", "")}</Household_Number>
            <Number_of_Trees>{data.get("householdFarmSize", "")}</Number_of_Trees>
            <Parent_Id>{data.get("trainingGroupId", "")}</Parent_Id>
            <Status>{data.get("status", "")}</Status>
            <Primary_Household_Member>{data.get("participantPrimaryHouseholdMember", "")}</Primary_Household_Member>
            <Case_Id>{data.get("commCareCaseId", "")}</Case_Id>
            <Name_Household_Concat>{fullname} {data.get("HHID", "")}-{farmernumber}</Name_Household_Concat>
            <n0:case case_id="{data.get("commCareCaseId", "")}" date_modified="{datetime.datetime.now()}" user_id="e926526fc13b126fffdb6d001f25b269" xmlns:n0="http://commcarehq.org/case/transaction/v2">
            <n0:create>
            <n0:case_name>{fullname}</n0:case_name>
            <n0:owner_id>{data.get("ccMobileWorkerGroupId", "")}</n0:owner_id>
            <n0:case_type>{case_type}</n0:case_type>
            </n0:create>
            <n0:update>
            <n0:Case_Id>{data.get("commCareCaseId", "")}</n0:Case_Id>
            <n0:First_Name>{firstname}</n0:First_Name>
            <n0:Middle_Name>{middlename}</n0:Middle_Name>
            <n0:Last_Name>{lastname}</n0:Last_Name>
            <n0:Age>{data.get("participantAge", "")}</n0:Age>
            <n0:Gender>{data.get("participantGender", "")}</n0:Gender>
            <n0:Phone_Number>{data.get("participantPhoneNumber", "")}</n0:Phone_Number>
            <n0:Farmer_Id>{data.get("tnsId", "")}</n0:Farmer_Id>
            <n0:Farmer_Number>{farmernumber}</n0:Farmer_Number>
            <n0:Cooperative_Membership_Number>{coopno}</n0:Cooperative_Membership_Number>
            <n0:Grower_Number>{grower_number}</n0:Grower_Number>
            <n0:National_ID_Number>{national_id_number}</n0:National_ID_Number>
            <n0:Household_Id>{data.get("householdId", "")}</n0:Household_Id>
            <n0:Household_Number>{data.get("HHID", "")}</n0:Household_Number>
            <n0:Number_of_Trees>{data.get("householdFarmSize", "")}</n0:Number_of_Trees>
            <n0:Status>{data.get("status", "")}</n0:Status>
            <n0:Primary_Household_Member>{data.get("participantPrimaryHouseholdMember", "")}</n0:Primary_Household_Member>
            <n0:Name_Household_Concat>{fullname} {data.get("HHID", "")}-{farmernumber}</n0:Name_Household_Concat>
            <n0:TNS_Id>{data.get("tnsId", "")}</n0:TNS_Id>
            <n0:Parent_Id>{data.get("trainingGroupId", "")}</n0:Parent_Id>
            </n0:update>
            <n0:index>
            <n0:parent case_type="{project_unique_id}_training_group">{data.get("trainingGroupId", "")}</n0:parent>
            </n0:index>
            </n0:case>
            <n1:meta xmlns:n1="http://openrosa.org/jr/xforms">
            <n1:deviceID>ID-10_t</n1:deviceID>
            <n1:timeStart>{first_time_stamp}</n1:timeStart>
            <n1:timeEnd>{datetime.datetime.now()}</n1:timeEnd>
            <n1:username>api</n1:username>
            <n1:userID>e926526fc13b126fffdb6d001f25b269</n1:userID>
            </n1:meta>
            </data>
            '''