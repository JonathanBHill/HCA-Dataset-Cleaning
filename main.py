#%% Imports
from modules.classes_old import Cleaning
import modules.constants_old as constants
from modules.constants_old import *
# from modules.constants_old import queries, mysql_engine
import sqlalchemy as sql


#%% Main
def main():
	mysql_engine = sql.engine.create_engine(
		"mysql+pymysql://rootds:@localhost:3306/HCA_Data"
	)
	test_query = queries[0]
	col, arguments = args[0]
	cleaning_obj = Cleaning(test_query, mysql_engine)
	cleaning_obj.correction = col, arguments
	cleaning_obj.cat_arr = category_array[0]
	cleaning_obj.order = edenc_order
	cleaning_obj.save_cols = keep_cols[0]
	cleaning_obj.clean_df()
	df_from_mysql = cleaning_obj.final_df
	print(df_from_mysql)


if __name__ == "__main__":
	main()
