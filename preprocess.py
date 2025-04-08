import pandas as pd

def calculate_weighted_average(group:pd.DataFrame, target_column:str, weight_column:str) -> float:
    """of the target column, using weight column as weight"""
    weighted_sum = (group[target_column] * group[weight_column]).sum()
    total_weight = group[weight_column].sum()    
    # print(weighted_sum / total_weight)
    return weighted_sum / total_weight

def count_inversions(list:list) -> int:
    count = 0
    for i in range(len(list)):
        for j in range(i + 1, len(list)):
            if list[i] < list[j]:
                count += 1
    return count

def process_and_append_inversions(
        output_df:pd.DataFrame,
        df:pd.DataFrame,
        target_column:str,
        weight_column:str = 'n_participants',
        diet_group_order:list[str] = ['meat100', 'meat', 'meat50', 'fish', 'veggie', 'vegan']
    ) -> None:
    """calculation is based on df, result is appended to output_df"""
    inversion_counts = {}
    groups = df.groupby('mc_run_id')

    for mc_id, group in groups:
        # sort in the order we given first
        ordered_group = pd.DataFrame()
        for diet in diet_group_order:
            diet_group_data = group[group['diet_group'] == diet]
            ordered_group = pd.concat([ordered_group, diet_group_data])

        weighted_averages = ordered_group.groupby('diet_group', sort=False).apply(
            lambda x: calculate_weighted_average(x, target_column = target_column, weight_column = weight_column),
            include_groups = False
        )
        
        inversion_count = count_inversions(weighted_averages.to_list())
        inversion_counts[mc_id] = inversion_count

    output_df[f'{target_column}_anomaly_level'] = output_df['mc_run_id'].map(inversion_counts)


# read csv
file_path = 'copy.csv'
df = pd.read_csv(file_path)

# target measure fields
target_columns = ['mean_ghgs','mean_land','mean_watscar','mean_eut','mean_ghgs_ch4','mean_ghgs_n2o','mean_bio','mean_watuse','mean_acid']

output_df = pd.DataFrame(list(range(1, 1001)), columns = ['mc_run_id'])
for target_column in target_columns:
    process_and_append_inversions(output_df, df, target_column)

# write to a new csv
output_file_path = 'anomaly_levels.csv'
output_df.to_csv(output_file_path, index=False)

print(f"process finished!")