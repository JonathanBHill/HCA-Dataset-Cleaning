from sqlalchemy import text, create_engine

queries = [
	text("select * from ED_Enc;"),
	text("select * from ED_Proc;"),
	text("select * from MS_Enc;"),
	text("select * from MS_Proc;")
]

mysql_engine = create_engine("mysql+pymysql://rootds:@localhost:3306/HCA_Data")

replacement_criteria = {
	"enc": ("Age_Years", (999, 91)),
	"proc": ("Rel_Service_Day", (-1, -1))
}

categorical_column_names = {
	"enc": {
		"HospID": "Hospital",
		"Payer_Type": "Payer_Cat",
		"Enc_Type": "Enc_Cat",
		"HCA_Disch_Disp_Desc": "Discharge_Cat"
	},
	"proc": {"HospID": "Hospital"}
}

order = {
	"enc": (
		"HospID", "Hospital", "PtID", "AdmtID", "Admit_Year", "Rel_Discharge_Date",
		"Reason_For_Visit", "Pat_ZIP_Masked", "Enc_Type", "Enc_Cat", "DRG",
		"DRG_Type", "HCA_Adm_Src", "HCA_Adm_Class", "HCA_Disch_Disp",
		"HCA_Disch_Disp_Desc", "Discharge_Cat", "Age_Years", "Payer_Type",
		"Payer_Cat"
	),
	"proc": (
		"HospID", "Hospital", "PtID", "AdmtID", "Proc_Seq_Num", "PX",
		"PX_Desc", "PX_CodeType", "OrigPX", "Rel_Service_Day"
	)
}

keep_cols = {
	"enc": (0, 1, 2, 3, 5, 8, 11, 12, 14, 15, 16, 17, 18, 19),
	"proc": (0, 1, 2, 3, 5, 6, 7, 9)
}

cleaned_names = (
	"edenc_cleaned.csv", "edproc_cleaned.csv", "msenc_cleaned.csv",
	"msproc_cleaned.csv"
)
