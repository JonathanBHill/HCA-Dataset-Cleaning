#%% Imports
from modules.classes_old import Cleaning
import modules.constants_old as constants
from modules.constants_old import queries, args
# from modules.constants_old import queries, mysql_engine
import sqlalchemy as sql


#%% Main
def main():
	mysql_engine = sql.engine.create_engine(
		"mysql+pymysql://rootds:@localhost:3306/HCA_Data"
	)
	# print(len(queries), queries)
	test_query = queries[0]
	# print(test_query.text)
	cleaning_obj = Cleaning(test_query, mysql_engine)
	cleaning_obj.correction = args
	cleaning_obj.clean_df()
	df_from_mysql = cleaning_obj.final_df
	print(df_from_mysql)


if __name__ == "__main__":
	main()
