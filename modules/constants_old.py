import sqlalchemy
from sqlalchemy import text, engine


mysql_engine = engine.create_engine(
	"mysql+pymysql://rootds:@localhost:3306/HCA_Data")

queries = [
	text("select * from ED_Enc;"),
	text("select * from ED_Proc;"),
	text("select * from MS_Enc;"),
	text("select * from MS_Proc;")
]

enc_type = {
	"Enc_Cat": {
		"E": 0,
		"I": 1,
		"O": 2,
		"S": 3
	}
}
hosp_type = {
	"Hospital": {
		26330: 0,
		27100: 1,
		27150: 2,
		27200: 3,
		27300: 4,
		27400: 5,
		27450: 6,
		31608: 7
	}
}
insurance_type = {
	"Payer_Cat": {
		"BLUE CROSS/COST": 0,
		"CHAMPUS": 0,
		"CHARITY": 2,
		"COMMERCIAL": 1,
		"FEDERAL": 0,
		"HEALTH EXCHANGE": 1,
		"HMO": 1,
		"MANAGED CARE MEDICAID": 0,
		"MANAGED CARE MEDICARE": 0,
		"MEDICAID": 0,
		"MEDICARE - DPU": 0,
		"MEDICARE - PPS": 0,
		"OTHER": 2,
		"PPO": 1,
		"SELF-PAY": 2,
		"STATE NON MCAID LOCL GOV": 0,
		"WORKERS COMP": 1
	}
}
discharge_type = {
	"Discharge_Cat": {
		"Admitted as an Inpatient to this Hospital": 0,
		"Discharged to Home or Self Care (Routine Discharge)": 1,
		"Discharged to home or self care with a planned acute care hospital inpatient readmission": 2,
		"Discharged/Transferred to a Facility that Provides Custodial or Supportive Care": 3,
		"Discharged/Trasnferred to Court/Law Enforcement": 4,
		"Discharged/transfer to an inpatient rehab facility (IRF) incl rehab distinct part units of a hospital with a planned acute care inpatient readmission": 5,
		"Discharged/transfer to another type of health care inst not defined elsewhere in this list with a planned acute care hospital readmission": 6,
		"Discharged/transferred to Home Under Care of Organized Home Health Service Organization in Anticipation of Covered Skilled Care": 7,
		"Discharged/transferred to Skilled Nursing Facility (SNF) with Medicare Certification in Anticipation of Covered Skilled Care": 8,
		"Discharged/transferred to a Designated Cancer Center or Children's Hospital": 9,
		"Discharged/transferred to a Federal Health Care Facility": 10,
		"Discharged/transferred to a Medicare Certified Long Term Care Hospital (LTCH)": 11,
		"Discharged/transferred to a Nursing Facility Certified under Medicaid but not Certified under Medicare": 12,
		"Discharged/transferred to a Psychiatric Hospital or Psychiatric Distinct Part Unit of a Hospital": 13,
		"Discharged/transferred to a Short-Term General Hospital for Inpatient Care": 14,
		"Discharged/transferred to a critical access hospital (CAH) with a planned acute care hospital inpatient readmission": 15,
		"Discharged/transferred to a designated cancer center or children's hospital with a planned acute care hospital inpatient readmission": 16,
		"Discharged/transferred to a nursing facility certified under Medicaid but not Medicare with a planned acute care inpatient readmission": 17,
		"Discharged/transferred to a psychiatric distinct part unit of a hospital with a planned acute care hospital inpatient readmission": 18,
		"Discharged/transferred to a short term general hospital for inpatient care with a planned acute care hospital inpatient readmission": 19,
		"Discharged/transferred to a skilled nursing facility (SNF) with Medicare certification with a planned acute care hospital inpatient readmission": 20,
		"Discharged/transferred to an Inpatient Rehabilitation Facility (IRF) including Rehabilitation Distinct Part Units of a Hospital": 21,
		"Discharged/transferred to another Type of Health Care Institution not Defined Elsewhere in this Code List": 22,
		"Discharged/transferred to court/law enforcement with a planned acute care hospital inpatient readmission": 23,
		"Discharged/transferred to home under care of organized home health service organization with a planned acute care hospital inpatient readmission": 24,
		"Expired": 25,
		"Hospice - Home": 26,
		"Hospice - Medical Facility (Certified) Providing Hospice Level of Care": 27,
		"Left Against Medical Advice or Discontinued Care": 28,
		"Still Patient": 29
	}
}

keep_cols = [
	[0, 1, 2, 3, 5, 8, 11, 12, 14, 15, 16, 17, 18, 19],
	[0, 1, 2, 3, 5, 6, 7, 9],
	[0, 1, 2, 3, 5, 8, 11, 12, 14, 15, 16, 17, 18, 19], [0, 1, 2, 3, 5, 6, 7, 9]
]

edenc_order = [
	"HospID", "Hospital", "PtID", "AdmtID", "Admit_Year", "Rel_Discharge_Date",
	"Reason_For_Visit", "Pat_ZIP_Masked", "Enc_Type", "Enc_Cat", "DRG",
	"DRG_Type", "HCA_Adm_Src", "HCA_Adm_Class", "HCA_Disch_Disp",
	"HCA_Disch_Disp_Desc", "Discharge_Cat", "Age_Years", "Payer_Type",
	"Payer_Cat"
]
msenc_order = [
	"HospID", "Hospital", "PtID", "AdmtID", "Admit_Year", "Rel_Discharge_Date",
	"Reason_For_Visit", "Pat_ZIP_Masked", "Enc_Type", "Enc_Cat", "DRG",
	"DRG_Type",
	"HCA_Adm_Src", "HCA_Adm_Class", "HCA_Disch_Disp", "HCA_Disch_Disp_Desc",
	"Discharge_Cat", "Age_Years", "Payer_Type", "Payer_Cat"
]
edproc_order = [
	"HospID", "Hospital", "PtID", "AdmtID", "Proc_Seq_Num", "PX",
	"PX_Desc", "PX_CodeType", "OrigPX", "Rel_Service_Day"
]
msproc_order = [
	"HospID", "Hospital", "PtID", "AdmtID", "Proc_Seq_Num", "PX",
	"PX_Desc", "PX_CodeType", "OrigPX", "Rel_Service_Day"
]
order_array = [edenc_order, edproc_order, msenc_order, msproc_order]

args = [
	["Age_Years", [999, 91]],
	["Rel_Service_Day"],
	["Age_Years", [999, 91]],
	["Rel_Service_Day"]
]

category_array = [
	[
		["HospID", hosp_type], ["Payer_Type", insurance_type],
		["Enc_Type", enc_type], ["HCA_Disch_Disp_Desc", discharge_type]
	],
	[
		["HospID", hosp_type]
	]
]

cleaned_names = [
	"edenc_cleaned.csv", "edproc_cleaned.csv", "msenc_cleaned.csv",
	"msproc_cleaned.csv"
]
