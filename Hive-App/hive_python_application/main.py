import hiveapp

# To create database
hiveapp.create_database('hive_challange')

# To create a csv temp table
hiveapp.create_table_csv_temp('department_data_temp_table')

# load data to csv table 
hiveapp.load_data('department_data_temp_table')

# create table orc permanent
hiveapp.create_table_orc_permanent('department_data_temp_table','department_data_perm_table')

# sort the table and show in dataframe (table_name, database_name)
hiveapp.show_sorted_dataframe('department_data_perm_table')

# drop the csv table
hiveapp.drop_temp_table('department_data_temp_table')

# to store the rows_data into list of tuples
hiveapp.df_rows_details('department_data_perm_table')
