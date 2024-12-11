def process_country_and_cohort(app_id):
    output_string = ''
    # Add more here
    app_dict = {
        'f079b0daae1d4d34a89e331dc5a72fbd': 'coffee_crew_2024',
        'd63cdcf6b9d849548413ca356871cd3a': 'coffee_hwg_2023',
        'e9fb922a1526447b9485b26c4a1b8eb5': 'coffee_regrow_2023',
        'dd10fc19040d40f0be48a447e1d2727c': 'coffee_regrow_2024',
        '30cee26f064e403388e334ae7b0c403b': 'coffee_ke_nn_2024a'
    }
    
    output_string = app_dict.get(app_id)
    
    return output_string