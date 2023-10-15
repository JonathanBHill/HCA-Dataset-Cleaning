import pandas as pd
from pandas import DataFrame, Series
from sqlalchemy import engine


def get_mysql_data(queries: list, sql_engine: engine):
	connection = sql_engine.connect()
	dfs = []
	if not isinstance(queries, list):
		queries = [queries]
	for index, query in enumerate(queries):
		dfs.append(pd.read_sql_query(query, connection))
		print(f"Populated dataframe {index + 1} of {len(queries)}.")
	connection.detach()
	connection.close()

	return dfs


class Cleaning:
	def __init__(
			self, dataframe: DataFrame, dataframe_type: str = None,
			replacements: tuple = None, cat_column_names: dict = None,
			order: tuple = None, columns_to_keep: tuple = None
	):
		self.df = dataframe
		self.df_type = dataframe_type
		self.replacement_values = replacements
		self.cat_col_names = cat_column_names
		self.column_order = list(order)
		self.keep_cols = list(columns_to_keep)
		self.final_df = self._clean_dataframe()

	def export_to_csv(self, filename: str = "cleaned.csv") -> None:
		self.final_df.to_csv(filename)
		print(f"Dataframe has been saved as {filename}.")

	def _clean_dataframe(self) -> DataFrame:
		if self.df_type == "enc":
			return self._clean_enc_dataframe()
		elif self.df_type == "proc":
			return self._clean_proc_dataframe()

	def _clean_proc_dataframe(self) -> DataFrame:
		self._replace_invalid_values(self.replacement_values)
		self._categorical_preparation()
		self._rearrange_columns(self.column_order)
		final_df = self.drop_columns(self.keep_cols)
		return final_df

	def _clean_enc_dataframe(
			self, format_column: str = "Payer_Type"
	) -> DataFrame:
		self._format_column(format_column)
		self._replace_invalid_values(self.replacement_values)
		self._categorical_preparation()
		self._rearrange_columns(self.column_order)
		final_df = self.drop_columns(self.keep_cols)
		return final_df

	def _format_column(self, col: str) -> None:
		if col == "Payer_Type":
			self.df[col] = self.df[col].apply(
				lambda x: x.rsplit(
					"\r",
					1
				)[0]
			)

	def _replace_invalid_values(
			self, replacement_criteria: tuple = None
	) -> None:
		col, replacements = replacement_criteria
		old, new = replacements
		self.df[col] = self.df[col].apply(lambda x: new if x == old else x)

	def _categorical_preparation(self) -> None:
		def get_categories(column: Series) -> dict:
			categories = {}
			unique_values = self.df[column].unique()
			unique_values.sort()
			for ID, category in enumerate(unique_values):
				categories[category] = ID
			return categories

		for old_col, new_col in self.cat_col_names.items():
			category_instructions = get_categories(old_col)
			self.df[new_col] = self.df[old_col].replace(category_instructions)

	def drop_columns(self, cols_to_keep) -> DataFrame:
		return self.df.iloc[:, cols_to_keep]

	def _rearrange_columns(self, order) -> DataFrame:
		self.df = self.df[order]
		return self.df
