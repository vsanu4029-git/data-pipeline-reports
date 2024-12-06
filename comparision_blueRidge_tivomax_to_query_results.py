import pandas as pd
query_results = pd.read_csv('C:/Users/BIRSTSA/Desktop/device_features_all_data.csv')
blue_stream_data = pd.read_csv('C:/Users/BIRSTSA/Desktop/Blue_Stream_tivomax_20241022.csv')
print(query_results.head())
print(blue_stream_data.head())

# # print("Query Results Columns:", query_results.columns)
# # print("Blue Stream Data Columns:", blue_stream_data.columns)


# # query_results.columns = ['TSN' if col == 'tsn' else col for col in query_results.columns]




# query_results.rename(columns={'tsn': 'TSN'}, inplace=True)
# query_results.rename(columns={
#     'Service State Desc': 'Service state Desc',
#     'Software Version': 'SW Version',
# }, inplace=True)


# # print(list(query_results.columns))

# # Reorder columns to match query_results
# blue_stream_data = blue_stream_data[query_results.columns]

# comparison = pd.merge(query_results, blue_stream_data, on='TSN', suffixes=('_query', '_file'))
# mismatches = comparison[
#     (comparison['Code Name_query'] != comparison['Code Name_file']) |
#     (comparison['External Product Name_query'] != comparison['External Product Name_file']) |
#     (comparison['Partner Parent Name_query'] != comparison['Partner Parent Name_file']) |
#     (comparison['Service State_query'] != comparison['Service State_file']) |
#     (comparison['SW Version_query'] != comparison['SW Version_file']) |
#     (comparison['Series_query'] != comparison['Series_file'])
# ]
# print(mismatches)

# mismatches.to_csv('C:/Users/BIRSTSA/Desktop/mismatched_output_all_data.csv')