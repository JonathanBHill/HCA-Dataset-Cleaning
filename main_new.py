# %% Imports
from modules.classes_new import get_mysql_data, Cleaning
from modules.constants_new import (
    queries, mysql_engine, replacement_criteria, categorical_column_names,
    order, keep_cols, cleaned_names
)


# %% Main
def main():
    """
    The main function to query data from a MySQL database, clean data as
    Pandas dataframes, and export them as CSV files.
    
    Returns:
        None
    """
    dataframes_from_mysql = get_mysql_data(queries, mysql_engine)
    
    for index, dataframe in enumerate(dataframes_from_mysql):
        if index % 2 == 0:
            df_type = "enc"
            print(
                f"Cleaning encounter dataframe {index} of"
                f" {len(dataframes_from_mysql)}"
            )
        else:
            df_type = "proc"
            print(
                f"Cleaning procedure dataframe {index} of"
                f" {len(dataframes_from_mysql)}"
            )
        
        cleaning_obj = Cleaning(
            dataframe, df_type,
            replacement_criteria[df_type],
            categorical_column_names[df_type],
            order[df_type], keep_cols[df_type]
        )
        
        print(cleaning_obj.final_df)
        cleaning_obj.export_to_csv(cleaned_names[index])
    
    print("All SQL tables have successfully been processed!")


if __name__ == "__main__":
    main()
